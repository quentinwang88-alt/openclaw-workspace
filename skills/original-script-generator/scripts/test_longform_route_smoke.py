"""One real call through the production longform route, without job writes."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from core.longform.voiceover import _invoke_voiceover_model, DEFAULT_MODEL_COMMAND


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--snapshot', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise ValueError('Output exists; do not repeat the paid call')
    frozen = json.loads(args.snapshot.read_text())
    payload = frozen['payloads']['full_case']
    result = _invoke_voiceover_model(dict(contract_name='creative_longform_single_v1', payload=payload), DEFAULT_MODEL_COMMAND)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result.get('_model_provenance'), ensure_ascii=False))
    print(result.get('chinese_translation'))


if __name__ == '__main__':
    main()
