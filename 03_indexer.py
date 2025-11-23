import json
from collections import Counter
import re

class IndexBuilder:
    def __init__(self):
        self.inverted_index = {}
        self.doc_freq = {}
        self.doc_lengths = {}
        self.documents = {}
        self.total_docs = 0

    def tokenize(self, text):
        if not text:
            return []
        text = text.lower()
        tokens = re.findall(r'\b[a-z]+\b', text)

        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}
        return [t for t in tokens if t not in stop_words]

    def build_index(self, jsonl_file):
        print(f"Building index from {jsonl_file}...")

        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                doc = json.loads(line)
                doc_id = self.total_docs

                title = doc.get('title', '')
                description = doc.get('description', '')
                text = f"{title} {description}"
                tokens = self.tokenize(text)

                term_freq = Counter(tokens)

                for term, freq in term_freq.items():
                    if term not in self.inverted_index:
                        self.inverted_index[term] = []
                        self.doc_freq[term] = 0

                    self.inverted_index[term].append((doc_id, freq))
                    self.doc_freq[term] += 1

                self.doc_lengths[doc_id] = len(tokens)
                self.documents[doc_id] = doc
                self.total_docs += 1

        self.avg_doc_length = sum(self.doc_lengths.values()) / len(self.doc_lengths) if self.doc_lengths else 0

        print(f"Index built: {len(self.inverted_index)} terms, {self.total_docs} documents")
        print(f"Average document length: {self.avg_doc_length:.2f} tokens")

    def save_index(self, output_file):
        print(f"Saving index to {output_file}...")

        with open(output_file, 'w', encoding='utf-8') as f:
            metadata = {
                'type': 'metadata',
                'total_docs': self.total_docs,
                'avg_doc_length': self.avg_doc_length
            }
            f.write(json.dumps(metadata) + '\n')

            for term, postings in self.inverted_index.items():
                term_data = {
                    'type': 'term',
                    'term': term,
                    'postings': postings,
                    'doc_freq': self.doc_freq[term]
                }
                f.write(json.dumps(term_data) + '\n')

            for doc_id, doc in self.documents.items():
                doc_data = {
                    'type': 'document',
                    'doc_id': doc_id,
                    'doc_length': self.doc_lengths[doc_id],
                    'data': doc
                }
                f.write(json.dumps(doc_data) + '\n')

        print(f"Index saved successfully!")

    def load_index(self, input_file):
        print(f"Loading index from {input_file}...")

        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)

                if data['type'] == 'metadata':
                    self.total_docs = data['total_docs']
                    self.avg_doc_length = data['avg_doc_length']

                elif data['type'] == 'term':
                    term = data['term']
                    self.inverted_index[term] = [tuple(p) for p in data['postings']]
                    self.doc_freq[term] = data['doc_freq']

                elif data['type'] == 'document':
                    doc_id = int(data['doc_id'])
                    self.documents[doc_id] = data['data']
                    self.doc_lengths[doc_id] = data['doc_length']

        print(f"Index loaded: {len(self.inverted_index)} terms, {self.total_docs} documents")


def main():
    import sys

    # Default paths
    input_file = 'data/parsed.jsonl'
    output_file = 'data/index.jsonl'

    # Parse command line arguments
    if len(sys.argv) >= 2:
        input_file = sys.argv[1]
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]

    builder = IndexBuilder()
    builder.build_index(input_file)
    builder.save_index(output_file)

    print(f"\n{'='*60}")
    print("Indexing complete!")
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
