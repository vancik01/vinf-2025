import json
import tiktoken

def count_tokens_in_jsonl(file_path: str, encoding_name: str = "cl100k_base") -> dict:
    # Initialize tiktoken encoding
    encoding = tiktoken.get_encoding(encoding_name)

    total_tokens = 0
    total_records = 0
    field_token_counts = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                total_records += 1

                for field_name, field_value in record.items():
                    if field_value is None:
                        text = "null"
                    elif isinstance(field_value, (int, float)):
                        text = str(field_value)
                    else:
                        text = str(field_value)

                    tokens = encoding.encode(text)
                    token_count = len(tokens)

                    if field_name not in field_token_counts:
                        field_token_counts[field_name] = 0
                    field_token_counts[field_name] += token_count

                    # Add to total
                    total_tokens += token_count

            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse line {line_num}: {e}")
                continue

    return {
        'total_tokens': total_tokens,
        'total_records': total_records,
        'field_token_counts': field_token_counts,
        'avg_tokens_per_record': total_tokens / total_records if total_records > 0 else 0
    }


def main():
    file_path = 'data/parsed.jsonl'

    print("Counting tokens from extracted.jsonl...")
    print("=" * 80)

    results = count_tokens_in_jsonl(file_path)

    print(f"\nTotal Records: {results['total_records']:,}")
    print(f"Average Tokens per Record: {results['avg_tokens_per_record']:.2f}")
    print("\nTokens by Field:")
    print("-" * 80)

    sorted_fields = sorted(results['field_token_counts'].items(),
                          key=lambda x: x[1],
                          reverse=True)

    for field_name, token_count in sorted_fields:
        print(f"  {field_name:25s}: {token_count:,} tokens")

    print("=" * 80)
    print(f"\n🎯 TOTAL TOKENS: {results['total_tokens']:,}")
    print("=" * 80)


if __name__ == "__main__":
    main()
