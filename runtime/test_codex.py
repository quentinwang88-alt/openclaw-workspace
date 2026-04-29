#!/usr/bin/env python3
"""Test codex mode image generation - runs as standalone script."""
import os, sys, json, base64, time

sys.path.insert(0, os.path.expanduser("~/.openclaw/workspace/skills/openai-image"))
sys.path.insert(0, os.path.expanduser("~/.openclaw/workspace"))

os.environ["OPENCLAW_AGENT_AUTH_PROFILE_PATH"] = os.path.expanduser(
    "~/.openclaw/agents/main/agent/auth-profiles.json"
)
os.environ["OPENAI_IMAGE_API_MODE"] = "codex"
os.environ["OPENAI_IMAGE_OUTPUT_DIR"] = os.path.expanduser("~/.openclaw/workspace/runtime/image_outputs")

from app.config import get_settings
import httpx

s = get_settings()
print(f"codex_api_key: {'present' if s.codex_api_key else 'EMPTY'}")
print(f"codex_base_url: {s.codex_base_url}")
print(f"codex_model: {s.codex_model}")

url = f"{s.codex_base_url}/responses"
headers = {
    "Authorization": f"Bearer {s.codex_api_key}",
    "Content-Type": "application/json",
}
body = {
    "model": "gpt-5.5",
    "instructions": "You are an image generation assistant.",
    "input": [{"role": "user", "content": [{"type": "input_text", "text": "A cute cat sitting on a windowsill"}]}],
    "tools": [{"type": "image_generation", "quality": "low", "size": "1024x1024", "output_format": "png"}],
    "stream": True,
    "store": False,
}

proxy_url = os.environ.get("HTTPS_PROXY", os.environ.get("HTTP_PROXY", ""))
print(f"Proxy: {proxy_url or 'none'}")
print("Sending request...")

start = time.time()
image_b64 = None
event_types = []
all_events = []

with httpx.Client(proxy=proxy_url or None, timeout=httpx.Timeout(300.0, connect=30.0)) as client:
    with client.stream("POST", url, json=body, headers=headers) as resp:
        print(f"Status: {resp.status_code} ({time.time()-start:.1f}s)")
        if resp.status_code != 200:
            err = resp.read().decode()[:500]
            print(f"Error: {err}")
            with open(os.path.expanduser("~/.openclaw/workspace/runtime/codex_error.json"), "w") as f:
                f.write(err)
            sys.exit(1)
        for line in resp.iter_lines():
            if not line or not line.startswith("data: "):
                continue
            data_str = line[6:]
            if data_str.strip() == "[DONE]":
                print("  Stream: [DONE]")
                break
            try:
                event = json.loads(data_str)
                et = event.get("type", "")
                event_types.append(et)
                all_events.append(event)

                # Print progress
                if et == "response.output_item.added":
                    item = event.get("item", {})
                    print(f"  + item type={item.get('type', '?')} ({time.time()-start:.1f}s)")
                elif et == "response.output_text.delta":
                    delta = event.get("delta", "")
                    if delta:
                        sys.stdout.write(delta)
                        sys.stdout.flush()
                elif et == "response.completed":
                    print(f"\n  Completed ({time.time()-start:.1f}s)")
                    for o in (event.get("response", {}).get("output") or []):
                        if isinstance(o, dict):
                            otype = o.get("type", "")
                            print(f"    output type={otype}")
                            if otype == "image_generation_call" and o.get("result"):
                                image_b64 = o["result"]
                                print(f"    Image b64 length={len(image_b64)}")
            except json.JSONDecodeError:
                pass

elapsed = time.time() - start
print(f"\nTotal: {elapsed:.1f}s, events: {len(event_types)}")

if image_b64:
    out_dir = os.path.expanduser("~/.openclaw/workspace/runtime/image_outputs")
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, "codex_test_cat.png")
    with open(out, "wb") as f:
        f.write(base64.b64decode(image_b64))
    print(f"SUCCESS: Image saved to {out} ({os.path.getsize(out)} bytes)")
else:
    print("FAILED: No image data found")
    print(f"Event types: {sorted(set(event_types))}")
    # Save events for debugging
    debug_path = os.path.expanduser("~/.openclaw/workspace/runtime/codex_stream_events.json")
    with open(debug_path, "w") as f:
        json.dump(all_events, f, indent=2, ensure_ascii=False)
    print(f"Events saved to {debug_path}")
