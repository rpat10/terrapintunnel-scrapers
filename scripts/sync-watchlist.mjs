// ─────────────────────────────────────────────────────────────────────────────
// sync-watchlist.mjs
// Intended to run every 5 minutes.
//
// Only scrapes the specific courses that users are actively watching.
// Much lighter than a full-term refresh — typically hits <50 unique courses
// vs. thousands. This keeps seat data fresh for the watchlist UI without
// hammering Testudo unnecessarily.
//
// Flow:
//   1. Query `watchlists` table → collect every watched section_id
//   2. Parse section_id ("CMSC131-202608-0101") → courseId + termId pairs
//   3. Group by (termId, courseId) to deduplicate — one Testudo request per course
//   4. Scrape seat counts for those courses
//   5. Upsert updated seat fields back into `sections`
// ─────────────────────────────────────────────────────────────────────────────

import { createClient } from '@supabase/supabase-js'
import * as cheerio from 'cheerio'
import dotenv from 'dotenv'
// Rotating pool of modern (2024–2025) browser User-Agent strings.
// A fresh UA is picked for every request to avoid a static bot fingerprint.
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
]

function randomUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]
}

dotenv.config({ path: '.env.local' })

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

// ─────────────────────────────────────────────────────────────────────────────
// BROWSER-LIKE HEADERS
// A real browser sends ~15 headers. Sending only User-Agent is itself a bot
// signal. These headers match what Chrome sends for a top-level navigation.
// ─────────────────────────────────────────────────────────────────────────────
function buildHeaders() {
  return {
    'User-Agent':                randomUserAgent(),
    'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language':           'en-US,en;q=0.9',
    'Accept-Encoding':           'gzip, deflate, br',
    'Connection':                'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest':            'document',
    'Sec-Fetch-Mode':            'navigate',
    'Sec-Fetch-Site':            'none',
    'Sec-Fetch-User':            '?1',
    'Cache-Control':             'max-age=0',
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STAGGERED BATCH EXECUTION
// Replaces Promise.all so requests within a batch don't all arrive at the
// server at the exact same millisecond — a textbook bot burst pattern.
// Each item starts ~50–100ms after the previous one.
// ─────────────────────────────────────────────────────────────────────────────
async function staggeredMap(items, fn) {
  return Promise.all(
    items.map((item, i) => {
      const delay = i === 0 ? 0 : i * (Math.floor(Math.random() * 50) + 50) // 50–100ms per slot
      return new Promise((resolve) => setTimeout(resolve, delay)).then(() => fn(item))
    })
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// RETRY HELPER
// Handles 429 Rate Limited explicitly: honors Retry-After if present, otherwise
// backs off aggressively. Other errors use standard exponential backoff.
// ─────────────────────────────────────────────────────────────────────────────
async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, options)

      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After') || '0', 10)
        const delay = retryAfter > 0 ? retryAfter * 1000 : Math.min(15000 * attempt, 60000)
        console.warn(`   ⚠️  Rate limited (429) — backing off ${(delay / 1000).toFixed(1)}s (attempt ${attempt}/${retries})`)
        if (attempt < retries) {
          await new Promise((r) => setTimeout(r, delay))
          continue
        }
      }

      if (!response.ok && attempt < retries) {
        const delay = Math.min(1000 * 2 ** (attempt - 1), 8000)
        console.warn(`   ⚠️  HTTP ${response.status} — retrying in ${delay}ms (attempt ${attempt}/${retries})`)
        await new Promise((r) => setTimeout(r, delay))
        continue
      }
      return response
    } catch (error) {
      if (attempt === retries) throw error
      const delay = Math.min(1000 * 2 ** (attempt - 1), 8000)
      console.warn(`   ⚠️  Network error — retrying in ${delay}ms (attempt ${attempt}/${retries}): ${error.message}`)
      await new Promise((r) => setTimeout(r, delay))
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PARSE SECTION ID
// Section IDs are stored as "{courseId}-{termId}-{sectionNumber}"
// termId is always a 6-digit number starting with "20" (e.g. 202608).
// ─────────────────────────────────────────────────────────────────────────────
function parseSectionId(sectionId) {
  const match = sectionId.match(/^(.+)-(20\d{4})-(.+)$/)
  if (!match) return null
  return { courseId: match[1], termId: match[2], sectionNumber: match[3] }
}

// ─────────────────────────────────────────────────────────────────────────────
// SCRAPE SEAT DATA FOR A SINGLE COURSE
// Returns an array of section objects with only the fields we need to update.
// ─────────────────────────────────────────────────────────────────────────────
async function scrapeSeatData(courseId, termId) {
  const url = `https://app.testudo.umd.edu/soc/search?courseId=${courseId}&sectionId=&termId=${termId}&_openSectionsOnly=on&creditCompare=&credits=&courseLevelFilter=ALL&instructor=&_facetoface=on&_blended=on&_online=on&courseStartCompare=&courseStartHour=&courseStartMin=&courseStartAM=&courseEndHour=&courseEndMin=&courseEndAM=&teachingCenter=ALL&_classDay1=on&_classDay2=on&_classDay3=on&_classDay4=on&_classDay5=on`

  try {
    const response = await fetchWithRetry(url, { headers: buildHeaders() })

    const html = await response.text()
    const $ = cheerio.load(html)

    // No sections found — course not offered this term (not an error)
    if (!$('.course').length && !$('.section').length) return []

    const sections = []

    $('.section').each((_, element) => {
      const sectionNumber = $(element).find('.section-id').text().trim()
      if (!sectionNumber) return

      const openSeats  = parseInt($(element).find('.open-seats-count').text().trim(), 10) || 0
      const totalSeats = parseInt($(element).find('.total-seats-count').text().trim(), 10) || 0
      // Use .first() — Testudo renders both Waitlist count AND Holdfile count
      // with the same class. Without .first(), "18" and "0" concatenate to "180".
      const waitlist   = parseInt($(element).find('.waitlist-count').first().text().trim(), 10) || 0

      sections.push({
        id:             `${courseId}-${termId}-${sectionNumber}`,
        course_id:      courseId,
        term_id:        termId,
        section_number: sectionNumber,
        open_seats:     openSeats,
        total_seats:    totalSeats,
        waitlist:       waitlist,
        updated_at:     new Date().toISOString(),
      })
    })

    return sections
  } catch (error) {
    console.error(`   ❌ Failed to scrape ${courseId} (${termId}): ${error.message}`)
    return []
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
async function runWatchlistSync() {
  const startTime = Date.now()
  console.log('==================================================')
  console.log('⚡ WATCHLIST SYNC: Refreshing watched sections')
  console.log(`   ${new Date().toISOString()}`)
  console.log('==================================================')

  // ── Step 1: Fetch all watched section IDs from the DB ──────────────────────
  const { data: watchlistRows, error: watchlistError } = await supabase
    .from('watchlists')
    .select('section_id')

  if (watchlistError) {
    console.error('❌ Failed to fetch watchlists:', watchlistError.message)
    process.exit(1)
  }

  if (!watchlistRows || watchlistRows.length === 0) {
    console.log('ℹ️  No sections on any watchlist. Nothing to do.')
    return
  }

  // ── Step 2: Parse IDs → unique (termId, courseId) pairs ────────────────────
  // Multiple users may watch different sections of the same course —
  // we only need one Testudo request per course per term.
  const courseMap = new Map() // key: "termId|courseId", value: { termId, courseId }

  for (const { section_id } of watchlistRows) {
    const parsed = parseSectionId(section_id)
    if (!parsed) {
      console.warn(`   ⚠️  Could not parse section_id: ${section_id}`)
      continue
    }
    const key = `${parsed.termId}|${parsed.courseId}`
    if (!courseMap.has(key)) {
      courseMap.set(key, { termId: parsed.termId, courseId: parsed.courseId })
    }
  }

  const uniqueCourses = Array.from(courseMap.values())
  console.log(`\n📋 ${watchlistRows.length} watchlist entries → ${uniqueCourses.length} unique courses to scrape\n`)

  // ── Step 3: Scrape with randomized batch size (3–5) and cooldown (200–500ms) ─
  // Cooldown is 200–500ms to offset the ~75–300ms of intra-batch stagger added
  // via staggeredMap, keeping requests spread out without inflating total runtime.
  const randBatchSize = () => Math.floor(Math.random() * 3) + 3  // 3–5 inclusive
  const randCooldown  = () => Math.floor(Math.random() * 301) + 200  // 200–500ms inclusive

  let totalUpdated = 0
  let failures = 0
  let i = 0

  while (i < uniqueCourses.length) {
    const batchSize = randBatchSize()
    const batch = uniqueCourses.slice(i, i + batchSize)

    const results = await staggeredMap(batch, ({ courseId, termId }) => scrapeSeatData(courseId, termId))

    const sectionsToUpsert = results.flat()

    if (sectionsToUpsert.length > 0) {
      // Deduplicate in case the same section appears multiple times
      const unique = Array.from(
        new Map(sectionsToUpsert.map((s) => [s.id, s])).values()
      )

      const { error } = await supabase
        .from('sections')
        .upsert(unique, { onConflict: 'id' })

      if (error) {
        console.error(`   ⚠️  Upsert error: ${error.message}`)
        failures++
      } else {
        totalUpdated += unique.length
        console.log(`   [${Math.min(i + batchSize, uniqueCourses.length)}/${uniqueCourses.length}] ✓ ${unique.length} sections updated`)
      }
    } else {
      console.log(`   [${Math.min(i + batchSize, uniqueCourses.length)}/${uniqueCourses.length}] (no open sections in batch)`)
    }

    i += batchSize

    if (i < uniqueCourses.length) {
      await new Promise((r) => setTimeout(r, randCooldown()))
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log('\n==================================================')
  console.log('📊 Watchlist Sync Summary')
  console.log('==================================================')
  console.log(`   Sections updated: ${totalUpdated}`)
  console.log(`   Upsert failures:  ${failures}`)
  console.log(`   Elapsed:          ${elapsed}s`)
  console.log('\n✅ Watchlist sync complete!')
}

runWatchlistSync()
