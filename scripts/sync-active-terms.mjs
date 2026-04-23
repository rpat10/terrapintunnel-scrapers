// ─────────────────────────────────────────────────────────────────────────────
// sync-active-terms.mjs
// Intended to run every 30 minutes.
//
// Performs a full section refresh for the two currently active semesters:
//   - Summer 2026 (202605)
//   - Fall 2026   (202608)
//
// Flow:
//   1. For each term, fetch each department page (/soc/{termId}/{deptCode})
//      → extract the list of course URLs from the "Show Sections" link hrefs
//      → this discovers all currently offered courses without needing JSON files,
//        so new courses added mid-semester are automatically picked up
//   2. Fetch each course page (/soc/{termId}/{deptCode}/{courseId})
//      → sections are fully embedded in the initial HTML response (no JS needed)
//   3. Parse and upsert all section records into the sections table
// ─────────────────────────────────────────────────────────────────────────────

import { createClient } from '@supabase/supabase-js'
import * as cheerio from 'cheerio'
import dotenv from 'dotenv'

dotenv.config({ path: '.env.local' })

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const ACTIVE_TERMS = [
  { termLabel: 'Summer 2026', termId: '202605' },
  { termLabel: 'Fall 2026',   termId: '202608' },
]

const DEPARTMENTS = [
  'AAAS', 'AAST', 'ABRM', 'AGNR', 'AGST', 'AMSC', 'AMST', 'ANSC', 'ANTH', 'AOSC',
  'ARAB', 'ARCH', 'AREC', 'ARHU', 'ARMY', 'ARSC', 'ARTH', 'ARTT', 'ASTR',
  'BCHM', 'BDBA', 'BIOE', 'BIOI', 'BIOL', 'BIOM', 'BIPH', 'BISI', 'BMGT', 'BMIN',
  'BMSO', 'BSCI', 'BSOS', 'BSST', 'BUAC', 'BUDT', 'BUFN', 'BULM', 'BUMK', 'BUSI',
  'BUSM', 'BUSO',
  'CBMG', 'CCJS', 'CHBE', 'CHEM', 'CHIN', 'CHPH', 'CHSE', 'CINE', 'CLAS', 'CLFS',
  'CMLT', 'CMNS', 'CMSC', 'COMM', 'CPBE', 'CPCV', 'CPDJ', 'CPET', 'CPGH', 'CPJT',
  'CPMS', 'CPPL', 'CPSA', 'CPSF', 'CPSG', 'CPSN', 'CPSP', 'CPSS', 'CRLN',
  'DANC', 'DATA',
  'ECON', 'EDCP', 'EDHD', 'EDHI', 'EDSP', 'EDUC', 'EMBA', 'ENAE', 'ENAI', 'ENBC',
  'ENCE', 'ENCO', 'ENEB', 'ENED', 'ENEE', 'ENES', 'ENFP', 'ENGL', 'ENMA', 'ENME',
  'ENMT', 'ENPM', 'ENRE', 'ENSE', 'ENSP', 'ENST', 'ENTM', 'ENTS', 'ENVH', 'EPIB',
  'FGSM', 'FIRE', 'FMSC', 'FREN',
  'GBHL', 'GEMS', 'GEOG', 'GEOL', 'GERS', 'GFPL', 'GREK', 'GVPT',
  'HACS', 'HBUS', 'HDCC', 'HEBR', 'HESI', 'HESP', 'HGLO', 'HHUM', 'HISP', 'HIST',
  'HLSA', 'HLSC', 'HLTH', 'HNUH', 'HONR',
  'IDEA', 'IMDM', 'IMMR', 'INAG', 'INFM', 'INST', 'ISRL', 'ITAL',
  'JAPN', 'JOUR', 'JWST',
  'KNES', 'KORA',
  'LACS', 'LARC', 'LATN', 'LBSC', 'LEAD', 'LGBT', 'LING',
  'MATH', 'MEES', 'MIEH', 'MITH', 'MLAW', 'MLSC', 'MSAI', 'MSML', 'MSQC', 'MUED',
  'MUSC',
  'NACS', 'NAVY', 'NEUR', 'NFSC', 'NIAS',
  'OURS',
  'PEER', 'PERS', 'PHIL', 'PHPE', 'PHSC', 'PHYS', 'PLCY', 'PLSC', 'PORT', 'PSYC',
  'QMMS',
  'RDEV', 'RELS', 'RUSS',
  'SDSB', 'SLAA', 'SLLC', 'SMLP', 'SOCY', 'SPAN', 'SPHL', 'STAT', 'SURV',
  'TDPS', 'THET', 'TLPL', 'TLTC',
  'UMEI', 'UNIV', 'URSP', 'USLT',
  'VIPS', 'VMSC',
  'WEID', 'WGSS',
  'XPER',
]

// ─────────────────────────────────────────────────────────────────────────────
// BROWSER-LIKE HEADERS
// ─────────────────────────────────────────────────────────────────────────────
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
// ─────────────────────────────────────────────────────────────────────────────
async function staggeredMap(items, fn) {
  return Promise.all(
    items.map((item, i) => {
      const delay = i === 0 ? 0 : i * (Math.floor(Math.random() * 50) + 50)
      return new Promise((resolve) => setTimeout(resolve, delay)).then(() => fn(item))
    })
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// RETRY HELPER
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
// STEP 1: DISCOVER COURSE URLS FOR A DEPARTMENT
// Fetches /soc/{termId}/{deptCode} and extracts the href from each course's
// "Show Sections" link. These hrefs are the direct per-course section URLs.
// Returns an array of absolute URLs like:
//   ["https://app.testudo.umd.edu/soc/202608/CMSC/CMSC131", ...]
// ─────────────────────────────────────────────────────────────────────────────
async function discoverCourseUrls(deptCode, termId) {
  const url = `https://app.testudo.umd.edu/soc/${termId}/${deptCode}`
  try {
    const response = await fetchWithRetry(url, { headers: buildHeaders() })
    const html = await response.text()
    const $ = cheerio.load(html)

    const urls = []
    $('.toggle-sections-link').each((_, el) => {
      const href = $(el).attr('href')
      if (href) urls.push(`https://app.testudo.umd.edu${href}`)
    })
    return urls
  } catch (error) {
    console.error(`   ❌ Failed to discover courses for ${deptCode} (${termId}): ${error.message}`)
    return []
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: SCRAPE SECTIONS FOR A SINGLE COURSE
// Fetches /soc/{termId}/{deptCode}/{courseId} — sections are fully embedded
// in the initial HTML, no JavaScript execution required.
// ─────────────────────────────────────────────────────────────────────────────
const scrapeFailures = []

async function scrapeSectionsForCourse(courseUrl, termId) {
  // Extract courseId from URL: /soc/202608/CMSC/CMSC131 → CMSC131
  const courseId = courseUrl.split('/').pop()

  try {
    const response = await fetchWithRetry(courseUrl, { headers: buildHeaders() })
    const html = await response.text()
    const $ = cheerio.load(html)

    if (!$('.section').length) return []

    const sections = []

    $('.section').each((_, element) => {
      const sectionNumber = $(element).find('.section-id').text().trim()
      if (!sectionNumber) return

      const instructorEls = $(element).find('.section-instructor')
      const instructors   = instructorEls.map((_, el) => $(el).text().trim()).get().filter(Boolean)
      const instructor    = instructors.length > 0 ? instructors.join(' / ') : ''

      const openSeats  = parseInt($(element).find('.open-seats-count').text().trim(), 10) || 0
      const totalSeats = parseInt($(element).find('.total-seats-count').text().trim(), 10) || 0
      const waitlist   = parseInt($(element).find('.waitlist-count').first().text().trim(), 10) || 0

      const meetings = []
      $(element).find('.class-days-container .row').each((_, meetingRow) => {
        const rowText = $(meetingRow).text()
        const type    = $(meetingRow).find('.class-type').text().trim() || 'Lecture'

        if (rowText.includes('Class time/details on ELMS')) {
          meetings.push({ days: 'Online', start_time: 'ASYNC', end_time: 'ASYNC', building: 'ONLINE', room: 'ELMS', type })
        } else {
          const days       = $(meetingRow).find('.section-days').text().trim()
          const start_time = $(meetingRow).find('.class-start-time').text().trim()
          const end_time   = $(meetingRow).find('.class-end-time').text().trim()
          const building   = $(meetingRow).find('.building-code').text().trim()
          const room       = $(meetingRow).find('.class-room').text().trim()
          if (days || start_time || room) {
            meetings.push({ days, start_time, end_time, building, room, type })
          }
        }
      })

      const hasAsync     = meetings.some((m) => m.start_time === 'ASYNC')
      const hasScheduled = meetings.some((m) => m.start_time && m.start_time !== 'ASYNC')
      const hasOnline    = meetings.some((m) => m.building === 'ONLINE' && m.start_time !== 'ASYNC')
      const section_type = hasAsync && hasScheduled ? 'hybrid'
        : hasAsync                                  ? 'async'
        : hasOnline                                 ? 'online'
        : 'in-person'

      sections.push({
        id:             `${courseId}-${termId}-${sectionNumber}`,
        course_id:      courseId,
        term_id:        termId,
        section_number: sectionNumber,
        instructor:     instructor || 'TBA',
        open_seats:     openSeats,
        total_seats:    totalSeats,
        waitlist:       waitlist,
        meeting_times:  meetings,
        section_type:   section_type,
        updated_at:     new Date().toISOString(),
      })
    })

    return sections
  } catch (error) {
    console.error(`   ❌ Failed to scrape ${courseId} (${termId}): ${error.message}`)
    scrapeFailures.push({ courseId, termId, reason: error.message })
    return []
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
async function runActiveTermsSync() {
  const startTime = Date.now()
  console.log('==================================================')
  console.log('🔄 ACTIVE TERMS SYNC: Full section refresh')
  console.log(`   ${new Date().toISOString()}`)
  console.log(`   Terms: ${ACTIVE_TERMS.map((t) => t.termLabel).join(', ')}`)
  console.log('==================================================\n')

  const randBatchSize = () => Math.floor(Math.random() * 3) + 3   // 3–5 inclusive
  const randCooldown  = () => Math.floor(Math.random() * 301) + 200 // 200–500ms inclusive

  let grandTotal = 0

  for (const { termLabel, termId } of ACTIVE_TERMS) {
    console.log(`🌐 ${termLabel} (${termId})`)
    console.log(`   Step 1: Discovering courses from ${DEPARTMENTS.length} department pages...`)

    // ── Step 1: Discover all course URLs across all departments ──────────────
    const allCourseUrls = []
    let dIdx = 0
    while (dIdx < DEPARTMENTS.length) {
      const batchSize = randBatchSize()
      const batch = DEPARTMENTS.slice(dIdx, dIdx + batchSize)
      const results = await staggeredMap(batch, (dept) => discoverCourseUrls(dept, termId))
      results.forEach((urls) => allCourseUrls.push(...urls))
      dIdx += batchSize
      await new Promise((r) => setTimeout(r, randCooldown()))
    }

    // Deduplicate (same course shouldn't appear under two depts, but be safe)
    const uniqueUrls = [...new Set(allCourseUrls)]
    console.log(`   Found ${uniqueUrls.length} courses.\n`)
    console.log(`   Step 2: Scraping sections for all ${uniqueUrls.length} courses...`)

    // ── Step 2: Scrape sections for each course ──────────────────────────────
    let termTotal = 0
    let cIdx = 0
    while (cIdx < uniqueUrls.length) {
      const batchSize = randBatchSize()
      const batch = uniqueUrls.slice(cIdx, cIdx + batchSize)
      const results = await staggeredMap(batch, (url) => scrapeSectionsForCourse(url, termId))
      const sectionsToUpsert = results.flat()

      if (sectionsToUpsert.length > 0) {
        const unique = Array.from(
          new Map(sectionsToUpsert.map((s) => [s.id, s])).values()
        )

        // Filter out sections whose course doesn't exist in the courses table
        const courseIds = [...new Set(unique.map((s) => s.course_id))]
        const { data: existingCourses } = await supabase
          .from('courses')
          .select('id')
          .in('id', courseIds)
        const validCourseIds = new Set((existingCourses || []).map((c) => c.id))
        const skipped = unique.filter((s) => !validCourseIds.has(s.course_id))
        const valid = unique.filter((s) => validCourseIds.has(s.course_id))

        if (skipped.length > 0) {
          const skippedIds = [...new Set(skipped.map((s) => s.course_id))]
          console.warn(`   ⚠️  Skipped ${skipped.length} sections — course(s) not in DB: ${skippedIds.join(', ')}`)
        }

        if (valid.length > 0) {
          const { error } = await supabase
            .from('sections')
            .upsert(valid, { onConflict: 'id' })

          if (error) {
            console.error(`   ⚠️  Upsert error: ${error.message}`)
          } else {
            termTotal += valid.length
          }
        }
      }

      cIdx += batchSize

      if (cIdx % 100 < batchSize) {
        console.log(`   [${Math.min(cIdx, uniqueUrls.length)}/${uniqueUrls.length} courses] ${termTotal} sections so far...`)
      }

      if (cIdx < uniqueUrls.length) {
        await new Promise((r) => setTimeout(r, randCooldown()))
      }
    }

    console.log(`✅ ${termLabel}: ${termTotal} sections updated.\n`)
    grandTotal += termTotal
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log('==================================================')
  console.log('📊 Active Terms Sync Summary')
  console.log('==================================================')
  console.log(`   Total sections updated: ${grandTotal}`)
  console.log(`   Scrape failures:        ${scrapeFailures.length}`)
  console.log(`   Elapsed:                ${elapsed}s`)

  if (scrapeFailures.length > 0) {
    console.warn('\n⚠️  Failed courses:')
    for (const f of scrapeFailures.slice(0, 20)) {
      console.warn(`   - ${f.courseId} (${f.termId}): ${f.reason}`)
    }
    if (scrapeFailures.length > 20) {
      console.warn(`   ... and ${scrapeFailures.length - 20} more`)
    }
  }

  console.log('\n🎉 Active terms sync complete!')
}

runActiveTermsSync()
