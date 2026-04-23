// ─────────────────────────────────────────────────────────────────────────────
// sync-active-terms.mjs
// Intended to run every 30 minutes (every hour midnight–6am UTC).
//
// Performs a full course + section refresh for the two currently active semesters:
//   - Summer 2026 (202605)
//   - Fall 2026   (202608)
//
// Flow:
//   1. Fetch each department page (/soc/{termId}/{deptCode})
//      → parse full course metadata (title, credits, gen-ed, prereqs, etc.)
//      → collect individual course page URLs for section scraping
//   2. Upsert all discovered courses into `courses` (satisfies FK before sections)
//   3. Fetch each course page individually (/soc/{termId}/{deptCode}/{courseId})
//      → sections are embedded in the full course page HTML (verified working)
//      → 100ms delay between requests keeps Testudo load gentle
//      → full run is ~15–20 minutes
//
// What gets updated every run:
//   - Open seats, total seats, waitlist counts
//   - Instructor / professor name
//   - Meeting times (days, start/end time)
//   - Location (building, room)
//   - Section type (in-person, async, online, hybrid)
//   - New courses and sections added mid-semester
//
// NOTE: avg_gpa and semesters_offered are intentionally excluded from course
// upserts — managed by sync-planetterp-monthly.mjs and separate tooling.
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
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function staggeredMap(items, fn) {
  return Promise.all(
    items.map((item, i) => {
      const delay = i === 0 ? 0 : i * (Math.floor(Math.random() * 50) + 50)
      return sleep(delay).then(() => fn(item))
    })
  )
}

async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, options)

      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After') || '0', 10)
        const delay = retryAfter > 0 ? retryAfter * 1000 : Math.min(15000 * attempt, 60000)
        console.warn(`   ⚠️  Rate limited (429) — backing off ${(delay / 1000).toFixed(1)}s (attempt ${attempt}/${retries})`)
        if (attempt < retries) { await sleep(delay); continue }
      }

      if (!response.ok && attempt < retries) {
        const delay = Math.min(1000 * 2 ** (attempt - 1), 8000)
        console.warn(`   ⚠️  HTTP ${response.status} — retrying in ${delay}ms (attempt ${attempt}/${retries})`)
        await sleep(delay)
        continue
      }
      return response
    } catch (error) {
      if (attempt === retries) throw error
      const delay = Math.min(1000 * 2 ** (attempt - 1), 8000)
      console.warn(`   ⚠️  Network error — retrying in ${delay}ms (attempt ${attempt}/${retries}): ${error.message}`)
      await sleep(delay)
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// COURSE METADATA PARSING
//
// Splits a raw course text block into structured fields by detecting trigger
// keywords (Prerequisite:, Restriction:, etc.) and treating everything before
// the first trigger as the course description.
// ─────────────────────────────────────────────────────────────────────────────
function parseMetaAndDescription(text) {
  text = text.replace(/\s+/g, ' ').trim()

  const triggers = [
    { key: 'prerequisites',           pattern: /Prerequisite\s*:/i },
    { key: 'restrictions',            pattern: /Restriction\s*:/i },
    { key: 'cross_listings',          pattern: /Cross-lists?ed with/i },
    { key: 'credit_only_granted_for', pattern: /Credit only granted for/i },
    { key: 'additional_information',  pattern: /Additional information\s*:/i },
    { key: 'formerly',                pattern: /Formerly\s*:/i },
    { key: 'corequisites',            pattern: /Corequisite\s*:/i },
    { key: 'recommended',             pattern: /Recommended\s*:/i },
  ]

  const foundMarks = []
  for (const { key, pattern } of triggers) {
    const re = new RegExp(pattern.source, 'gi')
    let m
    while ((m = re.exec(text)) !== null) {
      foundMarks.push({ key, start: m.index, end: m.index + m[0].length })
    }
  }
  foundMarks.sort((a, b) => a.start - b.start)

  const results = Object.fromEntries(triggers.map(({ key }) => [key, '']))
  results.description = text

  if (foundMarks.length === 0) return results

  results.description = text.slice(0, foundMarks[0].start).trim()

  for (let i = 0; i < foundMarks.length; i++) {
    const current = foundMarks[i]
    const startIndex = current.end
    const endIndex = i + 1 < foundMarks.length ? foundMarks[i + 1].start : text.length
    let content = text.slice(startIndex, endIndex).trim().replace(/^:/, '').trim()

    if (i === foundMarks.length - 1) {
      const proseSplit = content.match(/\.\s+([A-Z])/)
      if (proseSplit) {
        const splitIdx = content.indexOf(proseSplit[0])
        results[current.key] = content.slice(0, splitIdx + 1).trim()
        results.description = (results.description + ' ' + content.slice(splitIdx + 1).trim()).trim()
      } else {
        results[current.key] = content.replace(/\.$/, '').trim()
      }
    } else {
      results[current.key] = content
    }
  }

  for (const key of Object.keys(results)) {
    if (typeof results[key] === 'string') {
      results[key] = results[key].trim().replace(/^:/, '').trim()
    }
  }

  return results
}

// Extracts all course metadata from a single .course DOM node.
// Returns null if id or title are missing (both NOT NULL in DB).
// avg_gpa and semesters_offered are intentionally excluded.
function parseCourseBlock($, courseBlock) {
  const el = $(courseBlock)

  const courseId = el.find('.course-id').first().text().trim()
  if (!courseId) return null

  const title = el.find('.course-title').first().text().trim()
  if (!title) return null

  const minCreditsText = el.find('.course-min-credits').first().text().trim()
  const maxCreditsText = el.find('.course-max-credits').first().text().trim()
  const credits    = minCreditsText ? (parseInt(minCreditsText, 10) || null) : null
  const maxCredits = maxCreditsText ? (parseInt(maxCreditsText, 10) || null) : null

  // Clone before removing the label so we don't mutate the shared DOM
  let genEdString = null
  const genEdGroup = el.find('.gen-ed-codes-group > div').first()
  if (genEdGroup.length) {
    const cloned = genEdGroup.clone()
    cloned.find('.course-info-label').remove()
    const rawGenEd = cloned.text().replace(/\s+/g, ' ').trim().replace(/^:/, '').trim()
    if (rawGenEd) {
      genEdString = rawGenEd.replace(/([A-Z]{4})\s+(?=[A-Z]{4})/g, '$1, ')
    }
  }

  const courseData = {
    id:                      courseId,
    title,
    credits,
    max_credits:             maxCredits,
    gen_ed:                  genEdString,
    prerequisites:           null,
    restrictions:            null,
    corequisites:            null,
    recommended:             null,
    formerly:                null,
    cross_listings:          null,
    credit_only_granted_for: null,
    additional_information:  null,
    description:             null,
    updated_at:              new Date().toISOString(),
  }

  const descParts = []
  el.find('.approved-course-text, .course-text').each((_, tag) => {
    const rawText = $(tag).text().replace(/\s+/g, ' ').trim()
    if (!rawText) return
    const extracted = parseMetaAndDescription(rawText)

    if (extracted.prerequisites)           courseData.prerequisites           = extracted.prerequisites
    if (extracted.restrictions)            courseData.restrictions            = extracted.restrictions
    if (extracted.corequisites)            courseData.corequisites            = extracted.corequisites
    if (extracted.recommended)             courseData.recommended             = extracted.recommended
    if (extracted.formerly)               courseData.formerly                = extracted.formerly
    if (extracted.cross_listings)         courseData.cross_listings          = extracted.cross_listings
    if (extracted.credit_only_granted_for) courseData.credit_only_granted_for = extracted.credit_only_granted_for
    if (extracted.additional_information)  courseData.additional_information  = extracted.additional_information
    if (extracted.description)             descParts.push(extracted.description)
  })

  courseData.description = descParts.join(' ').trim() || null
  return courseData
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1: DISCOVER COURSES FOR A DEPARTMENT
// Fetches /soc/{termId}/{deptCode} once and extracts:
//   - Full course metadata from each .course block
//   - Individual course page URLs from "Show Sections" link hrefs
// ─────────────────────────────────────────────────────────────────────────────
async function discoverDepartment(deptCode, termId) {
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

    const courses = []
    $('.course').each((_, courseBlock) => {
      const course = parseCourseBlock($, courseBlock)
      if (course) courses.push(course)
    })

    return { urls, courses }
  } catch (error) {
    console.error(`   ❌ Failed to discover ${deptCode} (${termId}): ${error.message}`)
    return { urls: [], courses: [] }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: SCRAPE SECTIONS FOR A SINGLE COURSE
//
// Fetches the full course page (/soc/{termId}/{deptCode}/{courseId}).
// Sections are embedded in the initial HTML on this page — verified working.
//
// Updates per section:
//   - open_seats, total_seats, waitlist
//   - instructor (professor name, supports co-taught "Prof A / Prof B")
//   - meeting_times (days, start_time, end_time, building, room, type)
//   - section_type (in-person | async | online | hybrid)
// ─────────────────────────────────────────────────────────────────────────────
const scrapeFailures = []

async function scrapeSectionsForCourse(courseUrl, termId) {
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

      // Professor name — join multiple instructors with " / "
      const instructorEls = $(element).find('.section-instructor')
      const instructors   = instructorEls.map((_, el) => $(el).text().trim()).get().filter(Boolean)
      const instructor    = instructors.length > 0 ? instructors.join(' / ') : ''

      // Seat counts
      const openSeats  = parseInt($(element).find('.open-seats-count').text().trim(), 10) || 0
      const totalSeats = parseInt($(element).find('.total-seats-count').text().trim(), 10) || 0
      // .first() prevents "18" + "0" (holdfile) from concatenating to "180"
      const waitlist   = parseInt($(element).find('.waitlist-count').first().text().trim(), 10) || 0

      // Meeting times — days, start/end time, building, room, type
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
    console.error(`   ❌ Failed to scrape ${courseId}: ${error.message}`)
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
  console.log('🔄 ACTIVE TERMS SYNC: Full course + section refresh')
  console.log(`   ${new Date().toISOString()}`)
  console.log(`   Terms: ${ACTIVE_TERMS.map((t) => t.termLabel).join(', ')}`)
  console.log('==================================================\n')

  // Dept page batching: 3–5 per batch, 1–2s cooldown between batches (~3 min total)
  const randBatchSize  = () => Math.floor(Math.random() * 3) + 3      // 3–5
  const randDeptDelay  = () => Math.floor(Math.random() * 1001) + 1000 // 1–2s

  // Section scraping: sequential, 100ms between each request (~14 min for ~2500 courses)
  // Total estimated runtime: ~17 minutes
  const SECTION_DELAY_MS = 100

  // Upsert sections to DB every N courses to avoid holding too much in memory
  const SECTION_UPSERT_EVERY = 50

  let grandTotalSections = 0
  let grandTotalCourses  = 0

  for (const { termLabel, termId } of ACTIVE_TERMS) {
    console.log(`🌐 ${termLabel} (${termId})`)
    console.log(`   Step 1: Discovering courses from ${DEPARTMENTS.length} department pages...`)

    // ── Step 1: Fetch all dept pages, parse course metadata + collect URLs ────
    const allCourseUrls = []
    const courseMap     = new Map() // courseId → courseData (deduplicated)

    let dIdx = 0
    while (dIdx < DEPARTMENTS.length) {
      const batchSize = randBatchSize()
      const batch     = DEPARTMENTS.slice(dIdx, dIdx + batchSize)
      const results   = await staggeredMap(batch, (dept) => discoverDepartment(dept, termId))

      for (const { urls, courses } of results) {
        allCourseUrls.push(...urls)
        for (const course of courses) courseMap.set(course.id, course)
      }

      dIdx += batchSize
      if (dIdx < DEPARTMENTS.length) await sleep(randDeptDelay())
    }

    const uniqueUrls    = [...new Set(allCourseUrls)]
    const uniqueCourses = Array.from(courseMap.values())
    console.log(`   Found ${uniqueUrls.length} courses across ${uniqueCourses.length} unique course IDs.\n`)

    // ── Step 1b: Upsert courses before sections (satisfies FK constraint) ─────
    console.log(`   Upserting ${uniqueCourses.length} courses into DB...`)
    let coursesUpserted = 0
    for (let i = 0; i < uniqueCourses.length; i += 100) {
      const batch = uniqueCourses.slice(i, i + 100)
      const { error } = await supabase.from('courses').upsert(batch, { onConflict: 'id' })
      if (error) console.error(`   ⚠️  Course upsert error: ${error.message}`)
      else coursesUpserted += batch.length
    }
    console.log(`   ✓ ${coursesUpserted} courses upserted.\n`)
    grandTotalCourses += coursesUpserted

    // ── Step 2: Scrape each course page individually ──────────────────────────
    // Sequential with SECTION_DELAY_MS between requests — polite and reliable.
    // Sections flush to Supabase every SECTION_UPSERT_EVERY courses.
    console.log(`   Step 2: Scraping ${uniqueUrls.length} course pages (${SECTION_DELAY_MS}ms between each)...`)

    let termTotal      = 0
    let pendingSections = []

    for (let i = 0; i < uniqueUrls.length; i++) {
      const sections = await scrapeSectionsForCourse(uniqueUrls[i], termId)
      pendingSections.push(...sections)

      const isLast     = i === uniqueUrls.length - 1
      const shouldFlush = pendingSections.length >= SECTION_UPSERT_EVERY || isLast

      if (shouldFlush && pendingSections.length > 0) {
        const unique = Array.from(new Map(pendingSections.map((s) => [s.id, s])).values())
        const { error } = await supabase.from('sections').upsert(unique, { onConflict: 'id' })
        if (error) console.error(`   ⚠️  Section upsert error: ${error.message}`)
        else termTotal += unique.length
        pendingSections = []
      }

      if ((i + 1) % 100 === 0 || isLast) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0)
        console.log(`   [${i + 1}/${uniqueUrls.length} courses] ${termTotal} sections updated (${elapsed}s elapsed)`)
      }

      if (!isLast) await sleep(SECTION_DELAY_MS)
    }

    console.log(`✅ ${termLabel}: ${termTotal} sections updated.\n`)
    grandTotalSections += termTotal
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log('==================================================')
  console.log('📊 Active Terms Sync Summary')
  console.log('==================================================')
  console.log(`   Courses upserted:    ${grandTotalCourses}`)
  console.log(`   Sections updated:    ${grandTotalSections}`)
  console.log(`   Scrape failures:     ${scrapeFailures.length}`)
  console.log(`   Elapsed:             ${elapsed}s`)

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
