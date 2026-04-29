#!/usr/bin/env python3
"""Test codex mode image generation with full logging."""
import os, sys, json, base64, time

LOG = "/sessions/compassionate-elegant-pascal/mnt/workspace/runtime/codex_test.log"
RESULT = "/sessions/compassionate-elegant-pascal/mnt/workspace/runtime/codex_test_result.json"
IMAGE_OUT = "/sessions/compassionate-elegant-pascal/mnt/workspace/runtime/image_outputs/codex_test_cat.png"

def log(msg):
    print(msg, flush=True)
    with open(LOG, "a") as f:
        f.write(msg + "\n")

# Clear previous log
with open(LOG, "w") as f:
    f.write("")

log(f"Start: {time.strftime('%H:%M:%S')}")

# Read API key directly
auth_path = "/sessions/compassionate-elegant-pascal/mnt/agents/main/agent/auth-profiles.json"
try:
    with open(auth_path) as f:
        data = json.load(f)
    api_key = data.get("profiles", {}).get("openai-codex:default", {}).get("access", "")
except Exception as e:
    log(f"ERROR reading auth: {e}")
    sys.exit(1)

if not api_key:
    log("ERROR: No codex API key")
    sys.exit(1)

log(f"API key: present ({len(api_key)} chars)")

import httpx

url = "https://chatgpt.com/backend-api/codex/responses"
headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
body = {
    "model": "gpt-5.5",
    "instructions": "You are an image generation assistant.",
    "input": [{"role": "user", "content": [{"type": "input_text", "text": "A cute cat on a windowsill"}]}],
    "tools": [{"type": "image_generation", "quality": "low", "size": "1024x1024", "output_format": "png"}],
    "stream": True,
    "store": False,
}

proxy_url = os.environ.get("HTTPS_PROXY", os.environ.get("HTTP_PROXY", ""))
log(f"Proxy: {proxy_url or 'none'}")
log("Sending request...")

start = time.time()
image_b64 = None
event_types = []
all_events = []

try:
    with httpx.Client(proxy=proxy_url or None, timeout=httpx.Timeout(300.0, connect=30.0)) as client:
        with client.stream("POST", url, json=body, headers=headers) as resp:
            log(f"Status: {resp.status_code} ({time.time()-start:.1f}s)")
            if resp.status_code != 200:
                err_body = resp.read().decode()[:1000]
                log(f"Error: {err_body}")
                with open(RESULT, "w") as f:
                    json.dump({"status": "error", "code": resp.status_code, "body": err_body}, f, indent=2)
                sys.exit(1)

            for line in resp.iter_lines():
                if not line or not line.startswith("data: "):
                    continue
                ds = line[6:]
                if ds.strip() == "[DONE]":
                    log("Stream: [DONE]")
                    break
                try:
                    ev = json.loads(ds)
                    et = ev.get("type", "")
                    event_types.append(et)
                    all_events.append(ev)

                    if et == "response.output_item.added":
                        item = ev.get("item", {})
                        log(f"  + item: type={item.get('type', '?')} ({time.time()-start:.1f}s)")
                    elif et == "response.image_generation_call.completed":
                        # Try to get image from this event
                        item = ev.get("item", ev.get("output", {}))
                        if isinstance(item, dict) and item.get("result"):
                            image_b64 = item["result"]
                            log(f"  Image from completed event! b64 len={len(image_b64)} ({time.time()-start:.1f}s)")
                    elif et == "response.completed":
                        log(f"  Completed ({time.time()-start:.1f}s)")
                        for o in (ev.get("response", {}).get("output") or []):
                            if isinstance(o, dict):
                                otype = o.get("type", "")
                                log(f"    output type={otype}")
                                if otype == "image_generation_call" and o.get("result") and not image_b64:
                                    image_b64 = o["result"]
                                    log(f"    Image from completed! b64 len={len(image_b64)}")
                except json.JSONDecodeError:
                    pass

except Exception as e:
    log(f"Exception: {e}")
    with open(RESULT, "w") as f:
        json.dump({"status": "exception", "error": str(e)}, f, indent=2)
    sys.exit(1)

elapsed = time.time() - start
log(f"Total: {elapsed:.1f}s, events: {len(event_types)}")
log(f"Event types: {sorted(set(event_types))}")

if image_b64:
    os.makedirs(os.path.dirname(IMAGE_OUT), exist_ok=True)
    with open(IMAGE_OUT, "wb") as f:
        f.write(base64.b64decode(image_b64))
    log(f"SUCCESS: Image saved to {IMAGE_OUT} ({os.path.getsize(IMAGE_OUT)} bytes)")
    with open(RESULT, "w") as f:
        json.dump({"status": "success", "image_path": IMAGE_OUT, "size_bytes": os.path.getsize(IMAGE_OUT), "elapsed_seconds": round(elapsed, 1)}, f, indent=2)
else:
    log("FAILED: No image data found in response")
    with open(RESULT, "w") as f:
        json.dump({"status": "failed", "event_types": sorted(set(event_types)), "event_count": len(all_events)}, f, indent=2)
    # Save a sample of events for debugging (skip huge ones)
    debug_events = []
    for ev in all_events:
        et = ev.get("type", "")
        if et not in ("response.output_text.delta", "response.content_part.delta"):
            debug_events.append(ev)
    debug_path = "/sessions/compassionate-elegant-pascal/mnt/workspace/runtime/codex_stream_events.json"
    with open(debug_path, "w") as f:
        json.dump(debug_events, f, indent=2, ensure_ascii=False)
    log(f"Debug events saved ({len(debug_events)} events)")
