// ─────────────────────────────────────────────────────────────────────────────
// sync-planetterp-monthly.mjs
// Intended to run once per month.
//
// Syncs all data from the PlanetTerp API into Supabase:
//   • avg_gpa     → courses table   (from PlanetTerp /courses endpoint)
//   • prof_rating → sections table  (from PlanetTerp /professors endpoint)
//
// This is an enhanced version of sync-planetterp.mjs with:
//   - Explicit rate limiting (250ms between PlanetTerp pages)
//   - Batch concurrency cap on DB updates (50 at a time)
//   - Only overwrites existing values when PlanetTerp has a better one
//     (non-null avg_gpa / average_rating)
//   - Full pagination to handle PlanetTerp's 100-item page limit
//   - Graceful handling of partial/missing data
//
// PlanetTerp API docs: https://planetterp.com/api
// ─────────────────────────────────────────────────────────────────────────────

import { createClient } from '@supabase/supabase-js'
import dotenv from 'dotenv'

dotenv.config({ path: '.env.local' })

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const PLANETTERP_BASE = 'https://planetterp.com/api/v1'
const PT_PAGE_LIMIT    = 100   // PlanetTerp's max per-page result count
const PT_DELAY_MS      = 250   // Delay between PlanetTerp pages (be respectful)
const DB_UPDATE_CONCURRENCY = 50  // Concurrent Supabase UPDATE calls per batch

// ─────────────────────────────────────────────────────────────────────────────
// PLANETTERP PAGINATOR
// Fetches all pages from a PlanetTerp endpoint until exhausted.
// ─────────────────────────────────────────────────────────────────────────────
async function fetchAllFromPlanetTerp(endpoint) {
  const allData = []
  let offset = 0

  while (true) {
    const url = `${PLANETTERP_BASE}/${endpoint}?limit=${PT_PAGE_LIMIT}&offset=${offset}`

    const res = await fetch(url, {
      headers: { 'User-Agent': 'TerrapinTunnel/1.0 (university course tracker)' },
    })

    if (!res.ok) {
      console.error(`   ⚠️  PlanetTerp /${endpoint} returned HTTP ${res.status} at offset ${offset}`)
      break
    }

    const page = await res.json()
    if (!Array.isArray(page) || page.length === 0) break

    allData.push(...page)
    offset += PT_PAGE_LIMIT

    console.log(`   Fetched ${allData.length} ${endpoint} from PlanetTerp...`)

    if (page.length < PT_PAGE_LIMIT) break  // Last page — we're done

    await new Promise((r) => setTimeout(r, PT_DELAY_MS))
  }

  return allData
}

// ─────────────────────────────────────────────────────────────────────────────
// SUPABASE PAGINATOR
// Bypasses the default 1000-row limit.
// ─────────────────────────────────────────────────────────────────────────────
async function fetchAllFromSupabase(table, columns) {
  const allData = []
  let from = 0
  const PAGE_SIZE = 1000

  while (true) {
    const { data, error } = await supabase
      .from(table)
      .select(columns)
      .range(from, from + PAGE_SIZE - 1)

    if (error) throw error
    if (!data || data.length === 0) break

    allData.push(...data)
    if (data.length < PAGE_SIZE) break
    from += PAGE_SIZE
  }

  return allData
}

// ─────────────────────────────────────────────────────────────────────────────
// BATCH UPDATER
// Runs up to `concurrency` .update().eq() calls simultaneously.
// Only updates the specific columns in each row — never does a full overwrite.
// ─────────────────────────────────────────────────────────────────────────────
async function batchUpdate(table, rows, idCol, label, concurrency = DB_UPDATE_CONCURRENCY) {
  let saved = 0
  let failed = 0
  let errorSample = null

  for (let i = 0; i < rows.length; i += concurrency) {
    const chunk = rows.slice(i, i + concurrency)

    const results = await Promise.all(
      chunk.map((row) => {
        const { [idCol]: id, ...values } = row
        return supabase.from(table).update(values).eq(idCol, id)
      })
    )

    for (const { error } of results) {
      if (error) {
        failed++
        errorSample = errorSample ?? error.message
      } else {
        saved++
      }
    }
  }

  if (failed > 0) {
    console.error(`   ⚠️  ${label}: ${failed} update(s) failed. Sample error: ${errorSample}`)
  }

  return saved
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
async function runMonthlyPlanetTerpSync() {
  const startTime = Date.now()
  console.log('==================================================')
  console.log('🌍 PLANETTERP MONTHLY SYNC')
  console.log(`   ${new Date().toISOString()}`)
  console.log('==================================================\n')

  try {
    // ── Step 1: Sync Course Average GPAs ──────────────────────────────────────
    console.log('📊 Step 1: Syncing course average GPAs')
    console.log('   Fetching courses from PlanetTerp...')

    const ptCourses = await fetchAllFromPlanetTerp('courses')
    console.log(`   ✅ Received ${ptCourses.length} courses from PlanetTerp`)

    // Build a name → avg_gpa lookup (only include entries with actual GPA data)
    const gpaMap = new Map()
    for (const c of ptCourses) {
      if (c.name && c.average_gpa != null) {
        gpaMap.set(c.name, parseFloat(c.average_gpa.toFixed(3)))
      }
    }
    console.log(`   📐 GPA data available for ${gpaMap.size} courses`)

    // Fetch all course IDs from Supabase
    console.log('   Fetching course IDs from Supabase...')
    const dbCourses = await fetchAllFromSupabase('courses', 'id')
    console.log(`   Found ${dbCourses.length} courses in DB`)

    // Only queue an update when PlanetTerp has GPA data for this course
    const gpaUpdates = []
    for (const { id } of dbCourses) {
      const gpa = gpaMap.get(id)
      if (gpa != null) {
        gpaUpdates.push({ id, avg_gpa: gpa })
      }
    }

    console.log(`   💾 Updating avg_gpa for ${gpaUpdates.length} courses...`)
    const savedGpas = await batchUpdate('courses', gpaUpdates, 'id', 'Course GPA')
    console.log(`   ✅ Updated avg_gpa for ${savedGpas} courses\n`)

    // ── Step 2: Sync Professor Ratings ────────────────────────────────────────
    console.log('👨‍🏫 Step 2: Syncing professor ratings')
    console.log('   Fetching professors from PlanetTerp...')

    const ptProfs = await fetchAllFromPlanetTerp('professors')
    console.log(`   ✅ Received ${ptProfs.length} professors from PlanetTerp`)

    // Build a name → average_rating lookup
    const ratingMap = new Map()
    for (const p of ptProfs) {
      if (p.name && p.average_rating != null) {
        ratingMap.set(p.name, p.average_rating)
      }
    }
    console.log(`   📐 Rating data available for ${ratingMap.size} professors`)

    // Fetch only the fields we need from sections
    console.log('   Fetching section ids + instructors from Supabase...')
    const dbSections = await fetchAllFromSupabase('sections', 'id, instructor')
    console.log(`   Found ${dbSections.length} sections in DB`)

    // Build update list, averaging ratings for co-taught sections
    const ratingUpdates = []
    let multiInstructorCount = 0
    let noRatingCount = 0

    for (const { id, instructor } of dbSections) {
      if (!instructor || instructor === 'TBA') continue

      // Handle "Prof A / Prof B" format from master_sync
      const names   = instructor.split(' / ').map((n) => n.trim())
      const ratings = names.map((n) => ratingMap.get(n)).filter((r) => r != null)

      if (ratings.length === 0) {
        noRatingCount++
        continue
      }

      const avg = ratings.reduce((a, b) => a + b, 0) / ratings.length
      if (ratings.length > 1) multiInstructorCount++

      ratingUpdates.push({
        id,
        prof_rating: parseFloat(avg.toFixed(2)),
      })
    }

    console.log(`   💾 Updating prof_rating for ${ratingUpdates.length} sections`)
    console.log(`       (${multiInstructorCount} co-taught, ${noRatingCount} with no PlanetTerp data)`)

    const savedRatings = await batchUpdate('sections', ratingUpdates, 'id', 'Prof Rating')
    console.log(`   ✅ Updated prof_rating for ${savedRatings} sections\n`)

    // ── Summary ────────────────────────────────────────────────────────────────
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log('==================================================')
    console.log('📊 PlanetTerp Monthly Sync Summary')
    console.log('==================================================')
    console.log(`   Courses with GPA updated:       ${savedGpas}`)
    console.log(`   Sections with rating updated:   ${savedRatings}`)
    console.log(`   Sections with no rating data:   ${noRatingCount}`)
    console.log(`   Elapsed:                        ${elapsed}s`)
    console.log('\n🎉 PlanetTerp monthly sync complete!')

  } catch (error) {
    console.error('\n❌ Fatal sync error:', error.message)
    process.exitCode = 1
  }
}

runMonthlyPlanetTerpSync()
