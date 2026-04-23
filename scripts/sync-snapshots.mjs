// ─────────────────────────────────────────────────────────────────────────────
// sync-snapshots.mjs
// Intended to run once per day.
//
// Appends a time-series snapshot of open_seats / waitlist / total_seats for
// every section in the active terms. These snapshots power the seat-trend
// graph in the course details view.
//
// The `section_snapshots` table is append-only — each run adds a new row per
// section timestamped at the current time. The frontend queries this table
// ordered by `captured_at` to render the trend chart.
//
// Idempotency: Snapshots are guarded by a per-section, per-calendar-day check.
// If this script is run twice in the same day (e.g. after a failure + retry),
// it skips sections that already have a snapshot for today — no duplicates.
//
// Flow:
//   1. Determine today's date window (midnight → midnight UTC)
//   2. Fetch all section IDs in the active terms from Supabase
//   3. Fetch section IDs that already have a snapshot today → build skip set
//   4. Fetch current seat data for sections that still need a snapshot
//   5. Bulk insert new snapshots
// ─────────────────────────────────────────────────────────────────────────────

import { createClient } from '@supabase/supabase-js'
import dotenv from 'dotenv'

dotenv.config({ path: '.env.local' })

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

// Active terms — must match what's in sync-active-terms.mjs
const ACTIVE_TERM_IDS = ['202605', '202608']  // Summer 2026, Fall 2026

// ─────────────────────────────────────────────────────────────────────────────
// PAGINATION HELPER
// Supabase has a default 1000-row page limit. This fetches all pages.
// ─────────────────────────────────────────────────────────────────────────────
async function fetchAllPages(query) {
  let allData = []
  let from = 0
  const PAGE_SIZE = 1000

  while (true) {
    const { data, error } = await query.range(from, from + PAGE_SIZE - 1)
    if (error) throw error
    if (!data || data.length === 0) break
    allData.push(...data)
    if (data.length < PAGE_SIZE) break
    from += PAGE_SIZE
  }

  return allData
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
async function runSnapshotSync() {
  const startTime = Date.now()
  console.log('==================================================')
  console.log('📸 SNAPSHOT SYNC: Daily seat history capture')
  console.log(`   ${new Date().toISOString()}`)
  console.log(`   Active terms: ${ACTIVE_TERM_IDS.join(', ')}`)
  console.log('==================================================\n')

  // ── Step 1: Build today's date window (UTC midnight → midnight) ────────────
  const now   = new Date()
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0))
  const end   = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 0, 0))

  console.log(`📅 Snapshot window: ${start.toISOString()} → ${end.toISOString()}`)

  // ── Step 2: Fetch all sections in active terms ─────────────────────────────
  console.log('\n🔍 Fetching active sections from DB...')
  const sections = await fetchAllPages(
    supabase
      .from('sections')
      .select('id, open_seats, waitlist, total_seats')
      .in('term_id', ACTIVE_TERM_IDS)
  )
  console.log(`   Found ${sections.length} sections across active terms`)

  if (sections.length === 0) {
    console.log('ℹ️  No sections found. Nothing to snapshot.')
    return
  }

  // ── Step 3: Find which sections already have a snapshot today ─────────────
  // Query in batches of 500 section IDs to avoid URL length limits
  console.log('\n🔎 Checking for existing snapshots today...')
  const allSectionIds = sections.map((s) => s.id)
  const alreadySnapshotted = new Set()
  const ID_BATCH = 500

  for (let i = 0; i < allSectionIds.length; i += ID_BATCH) {
    const batchIds = allSectionIds.slice(i, i + ID_BATCH)
    const { data: existing, error } = await supabase
      .from('section_snapshots')
      .select('section_id')
      .in('section_id', batchIds)
      .gte('captured_at', start.toISOString())
      .lt('captured_at', end.toISOString())

    if (error) {
      console.error(`   ⚠️  Error checking existing snapshots: ${error.message}`)
      continue
    }

    for (const row of existing) {
      alreadySnapshotted.add(row.section_id)
    }
  }

  console.log(`   ${alreadySnapshotted.size} sections already snapshotted today — skipping`)

  // ── Step 4: Build snapshot rows for sections that still need one ───────────
  const capturedAt = new Date().toISOString()

  const snapshotsToInsert = sections
    .filter((s) => !alreadySnapshotted.has(s.id))
    .map((s) => ({
      section_id:  s.id,
      open_seats:  s.open_seats  ?? 0,
      waitlist:    s.waitlist    ?? 0,
      total_seats: s.total_seats ?? 0,
      captured_at: capturedAt,
    }))

  console.log(`\n💾 Inserting ${snapshotsToInsert.length} new snapshots...`)

  if (snapshotsToInsert.length === 0) {
    console.log('ℹ️  All sections already have a snapshot for today.')
    return
  }

  // ── Step 5: Bulk insert in batches of 500 ─────────────────────────────────
  const INSERT_BATCH = 500
  let inserted = 0
  let failed = 0

  for (let i = 0; i < snapshotsToInsert.length; i += INSERT_BATCH) {
    const batch = snapshotsToInsert.slice(i, i + INSERT_BATCH)

    const { error } = await supabase
      .from('section_snapshots')
      .insert(batch)

    if (error) {
      console.error(`   ⚠️  Insert error (batch ${Math.floor(i / INSERT_BATCH) + 1}): ${error.message}`)
      failed += batch.length
    } else {
      inserted += batch.length
      console.log(`   [${Math.min(i + INSERT_BATCH, snapshotsToInsert.length)}/${snapshotsToInsert.length}] inserted...`)
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log('\n==================================================')
  console.log('📊 Snapshot Sync Summary')
  console.log('==================================================')
  console.log(`   Snapshots inserted:  ${inserted}`)
  console.log(`   Already existed:     ${alreadySnapshotted.size}`)
  console.log(`   Failed:              ${failed}`)
  console.log(`   Elapsed:             ${elapsed}s`)
  console.log('\n🎉 Snapshot sync complete!')
}

runSnapshotSync()
