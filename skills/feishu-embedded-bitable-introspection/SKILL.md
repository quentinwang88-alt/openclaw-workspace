---
name: feishu-embedded-bitable-introspection
description: Recover embedded Feishu wiki bitable metadata, field mappings, and record data from a logged-in local browser session when the official app scopes or browser automation path are blocked.
author: Hermes Agent
---

# Feishu embedded bitable introspection

Use this when:
- The user gives a Feishu wiki/table URL like `.../wiki/<token>?table=<tableId>&view=<viewId>&sheet=<sheetId>`
- Browser automation is flaky or unavailable
- Official Feishu OpenAPI scopes are missing for wiki/bitable access
- You still have local access to the machine's logged-in Chrome profile

## What this solves
This workflow can recover:
- wiki title / table title
- embedded base token
- table/view/sheet IDs
- field IDs and option mappings
- full `recordMap` data for the target table/view
- unfinished-record detection logic

It does **not** automatically guarantee writeback; writing often requires additional reverse-engineering of the internal changeset/send-message path.

## Preconditions
- Local machine has an authenticated Chrome session for Feishu
- Python available
- Network access to Feishu web endpoints
- macOS/Chrome path assumptions may need adjustment on other OSes

## Core findings
For embedded wiki sheets, the public wiki HTML often contains enough SSR/bootstrap data to recover the real embedded base token and then fetch full clientvars.

Key pattern:
1. Fetch the wiki URL with Chrome cookies
2. Read SSR bootstrap from HTML
3. Extract `activeSheet`, `sheetId`, and especially a 27-char embedded base token from `gzipTopSnapshot`
4. Call:
   `GET /space/api/v1/bitable/<baseToken>/clientvars?tableID=<tableId>&viewID=<viewId>&needBase=true&recordLimit=500&ondemandLimit=500`
5. Decode gzip+base64 `data.table`
6. Parse JSON to get `fieldMap`, `recordMap`, `viewMap`, etc.

## Recommended workflow

### 1. Reuse local Chrome cookies
Install helper if needed:
```bash
python3 -m pip install browser-cookie3 keyring
```

Load cookies in Python:
```python
import browser_cookie3, requests
s = requests.Session()
s.cookies.update(browser_cookie3.chrome())
```

### 2. Fetch the wiki page directly
```python
html = s.get(WIKI_URL, timeout=30).text
```
If plain requests without cookies redirects to login, retry with `browser_cookie3` cookies.

### 3. Extract SSR sheet metadata from HTML
Look for `window.DATA={clientVars:...}` and `gzipTopSnapshot`.
Useful clues from the HTML include:
- `activeSheet`
- `sheetId`
- `window.SERVER_DATA.meta.title`
- `window.current_space_wiki`

The important compressed blob is usually `gzipTopSnapshot` inside the SSR payload.

### 4. Decode `gzipTopSnapshot`
Example:
```python
import re, gzip, base64
m = re.search(r'gzipTopSnapshot":"([A-Za-z0-9+/=\\u003d]+)"', html)
enc = m.group(1).replace('\\u003d', '=')
dec = gzip.decompress(base64.b64decode(enc)).decode('utf-8', 'ignore')
print(dec)
```

This may reveal a string like:
- `<baseToken>_<tableId>`

In the observed case, decoding revealed entries such as:
- `L8b1bRq2Oa2Uy9syZzqclJb7nmf_tbltMvS0a8hotiIk`

The 27-character prefix is the embedded base token.

### 5. Fetch full table clientvars
Use the embedded base token, not the wiki token.

```python
r = s.get(
    f'https://<tenant>.feishu.cn/space/api/v1/bitable/{base_token}/clientvars',
    params={
        'tableID': table_id,
        'viewID': view_id,
        'needBase': 'true',
        'recordLimit': '500',
        'ondemandLimit': '500',
    },
    timeout=30,
)
j = r.json()
```

If successful, `j['data']['table']` is usually gzip+base64.

### 6. Decode the table payload
```python
import json, gzip, base64
obj = json.loads(gzip.decompress(base64.b64decode(j['data']['table'])).decode('utf-8', 'ignore'))
```

Important keys:
- `obj['meta']`
- `obj['fieldMap']`
- `obj['recordMap']`
- `obj['viewMap']`
- `obj['resourceMap']`

### 7. Build field name ↔ field ID mapping
```python
name_to_id = {v['name']: k for k, v in obj['fieldMap'].items()}
id_to_name = {k: v['name'] for k, v in obj['fieldMap'].items()}
```

For single-select / multi-select fields, inspect:
```python
obj['fieldMap'][field_id]['property']['options']
```
This lets you map option IDs back to human-readable labels.

### 8. Identify unfinished rows
Common pattern:
```python
for rid, rec in obj['recordMap'].items():
    feat = rec.get(name_to_id['特征打点JSON'], {}).get('value')
    rating = rec.get(name_to_id['产品评级'], {}).get('value')
    action = rec.get(name_to_id['建议动作'], {}).get('value')
    if not feat or not action:
        ...
```

Adjust logic to the user’s actual definition of “unfinished”.

## Fast verification checklist
- HTML fetch with cookies returns wiki content, not login page
- `gzipTopSnapshot` decodes successfully
- Extracted base token is 27 chars
- `clientvars` request returns `code: 0`
- Decoded `table.meta.id` matches requested `tableID`
- `recordMap` size matches or closely tracks `meta.recordsNum`

## Pitfalls
- **Official OpenAPI may fail even when browser session works**: missing scopes like `wiki:node:read` block app-token access.
- **Playwright/browser automation may be unnecessary**: direct HTTP with Chrome cookies is often faster and more reliable.
- **The wiki token is not the embedded base token**: using the wrong token returns `Not Found` or unrelated data.
- **Embedded table metadata may not appear plainly in HTML**: decode `gzipTopSnapshot`; don’t rely only on visible strings.
- **Some candidate 27-char tokens from IndexedDB are red herrings**: verify by calling `clientvars` and checking that decoded `meta.id` matches the target table.
- **Single-select fields often store option IDs, not labels**: for fields like `分析状态`, record values may look like `opt7dDgAOg` rather than `已完成分析`. Always decode through `fieldMap[field_id]['property']['options']` before counting pending/completed rows.
- **`execute_code` may not see user-site packages even when `pip install --user` says they exist**: if `browser_cookie3` import fails in the code sandbox, retry with `terminal` + `python3` from the live environment instead of assuming the package is actually missing.
- **Writeback is harder than readback**: reading via `clientvars` is straightforward once the base token is known; writeback typically requires reverse-engineering internal changeset/send-message endpoints.

## Notes from observed run
Observed successful path:
- Feishu wiki URL provided with `table`, `view`, and `sheet`
- `window.SERVER_DATA.meta.title` exposed the document title
- `gzipTopSnapshot` exposed `<baseToken>_<tableId>`
- `GET /space/api/v1/bitable/<baseToken>/clientvars` with `tableID` and `viewID` returned full table payload
- Decoded payload contained `fieldMap`, `recordMap`, and select-option IDs sufficient to identify unfinished records and prepare structured writeback
