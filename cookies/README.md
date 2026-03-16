# HLTV Cookie Setup (One-Time)

This folder stores your HLTV browser cookies so the scraper can bypass
Cloudflare without solving a CAPTCHA every time.

---

## Step-by-step (do this once, repeat when cookies expire)

### 1. Install the browser extension
- Chrome: [Cookie-Editor](https://chrome.google.com/webstore/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm)
- Firefox: [Cookie-Editor](https://addons.mozilla.org/en-US/firefox/addon/cookie-editor/)

### 2. Visit HLTV and pass Cloudflare
1. Open Chrome/Firefox
2. Go to **https://www.hltv.org/betting/money**
3. If a Cloudflare CAPTCHA appears → solve it
4. Wait until the full betting page loads (you should see match odds)

### 3. Export cookies
1. Click the Cookie-Editor extension icon
2. Click **Export** → **Export as JSON**
3. Copy all the text

### 4. Save the cookies file
- Paste the copied JSON into this file: `cookies/hltv_cookies.json`
- Save the file

### 5. Also save your User-Agent (IMPORTANT)
- In Chrome: open DevTools (F12) → Network tab → refresh page
  → click any request → Headers → find `User-Agent`
- Paste it into `cookies/hltv_useragent.txt`

---

## How long do cookies last?
- Cloudflare `__cf_clearance` cookie: typically **1–24 hours**
- If the scraper starts returning no odds → repeat steps 2–4

## Signs cookies expired
- Logs show: `[HLTV BETTING] HTTP 403` or `HTTP 503`
- No real odds in the app — falls back to generated odds

---

## Security
- `hltv_cookies.json` is in `.gitignore` — it will NOT be committed to git
- Never share your cookies file with anyone
