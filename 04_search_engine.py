import json
import math
import re
from typing import List, Tuple, Dict


class SearchEngine:
    def __init__(self, index_file):
        self.inverted_index = {}
        self.doc_freq = {}
        self.doc_lengths = {}
        self.documents = {}
        self.total_docs = 0
        self.avg_doc_length = 0

        self._load_index(index_file)

    def _load_index(self, index_file):
        with open(index_file, 'r', encoding='utf-8') as f:
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

    def tokenize(self, text):
        if not text:
            return []
        text = text.lower()
        tokens = re.findall(r'\b[a-z]+\b', text)
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}
        return [t for t in tokens if t not in stop_words]

    def calculate_idf(self, df: int, method: str = 'bm25') -> float:
        if method == 'bm25':
            return math.log((self.total_docs - df + 0.5) / (df + 0.5) + 1.0)
        elif method == 'tfidf':
            return math.log(self.total_docs / df)

    def calculate_bm25_score(self, query_terms, doc_id):
        score = 0
        k1 = 1.2
        b = 0.75
        doc_len = self.doc_lengths.get(doc_id, 0)

        for term in query_terms:
            if term not in self.inverted_index:
                continue

            tf = 0
            for d_id, freq in self.inverted_index[term]:
                if d_id == doc_id:
                    tf = freq
                    break

            if tf == 0:
                continue

            df = self.doc_freq[term]
            idf = self.calculate_idf(df, method='bm25')

            numerator = tf * (k1 + 1)
            denominator = tf + k1 * (1 - b + b * doc_len / self.avg_doc_length)
            score += idf * (numerator / denominator)

        return score

    def calculate_tfidf_score(self, query_terms, doc_id):
        score = 0
        doc_len = self.doc_lengths.get(doc_id, 0)

        if doc_len == 0:
            return 0

        for term in query_terms:
            if term not in self.inverted_index:
                continue

            tf = 0
            for d_id, freq in self.inverted_index[term]:
                if d_id == doc_id:
                    tf = freq
                    break

            if tf == 0:
                continue

            df = self.doc_freq[term]
            idf = self.calculate_idf(df, method='tfidf')

            tf_normalized = tf / doc_len
            score += tf_normalized * idf

        return score

    def calculate_score(self, query_terms, doc_id, method='bm25'):
        if method == 'bm25':
            return self.calculate_bm25_score(query_terms, doc_id)
        elif method == 'tfidf':
            return self.calculate_tfidf_score(query_terms, doc_id)
        else:
            # Default to BM25
            return self.calculate_bm25_score(query_terms, doc_id)

    def limit_words(self, text: str, max_words: int = 20) -> str:
        if not text:
            return 'N/A'
        words = text.split()
        if len(words) <= max_words:
            return text
        return ' '.join(words[:max_words]) + '...'

    def calculate_term_metrics(self, query_terms: List[str], doc_id: int) -> Dict:
        metrics = {}
        doc_len = self.doc_lengths.get(doc_id, 0)

        for term in query_terms:
            if term not in self.inverted_index:
                metrics[term] = {
                    'tf': 0,
                    'df': 0,
                    'idf': 0,
                    'present': False
                }
                continue

            tf = 0
            for d_id, freq in self.inverted_index[term]:
                if d_id == doc_id:
                    tf = freq
                    break

            df = self.doc_freq[term]
            idf = math.log(self.total_docs / df) if df > 0 else 0

            metrics[term] = {
                'tf': tf,
                'df': df,
                'idf': round(idf, 4),
                'tf_normalized': round(tf / doc_len, 4) if doc_len > 0 else 0,
                'present': tf > 0
            }

        return metrics

    def search(self, query: str, method='bm25', top_k=10) -> List[Dict]:
        query_terms = self.tokenize(query)

        if not query_terms:
            print("No valid query terms after tokenization")
            return []

        print(f"Query terms: {query_terms}")

        candidate_docs = set()
        for term in query_terms:
            if term in self.inverted_index:
                for doc_id, _ in self.inverted_index[term]:
                    candidate_docs.add(doc_id)

        print(f"Found {len(candidate_docs)} candidate documents")

        results = []
        for doc_id in candidate_docs:
            score = self.calculate_score(query_terms, doc_id, method=method)
            results.append((doc_id, score))

        results.sort(key=lambda x: x[1], reverse=True)

        formatted_results = []
        for doc_id, score in results[:top_k]:
            doc = self.documents[doc_id]
            term_metrics = self.calculate_term_metrics(query_terms, doc_id)

            formatted_results.append({
                'doc_id': doc_id,
                'score': round(score, 6),
                'title': doc.get('title', 'N/A'),
                'price': doc.get('price', 'N/A'),
                'property_type': doc.get('property_type', 'N/A'),
                'beds': doc.get('beds'),
                'baths': doc.get('baths'),
                'sqft': doc.get('sqft'),
                'city': doc.get('city', 'N/A'),
                'county': doc.get('county', 'N/A'),
                'url': doc.get('url', 'N/A'),
                'description': self.limit_words(doc.get('description', ''), max_words=20),
                'file_hash': doc.get('file_hash', 'N/A'),
                'property_id': doc.get('property_id', 'N/A'),
                'wiki_data': doc.get('wiki_data'),
                'term_metrics': term_metrics,
                'method': method
            })

        return formatted_results


def display_results(results: List[Dict]):
    if not results:
        print("No results found")
        return

    for i, result in enumerate(results, 1):
        print(f"\nRESULT #{i} | Score: {result['score']} ({result['method'].upper()})")
        print(f"\nTitle: {result['title']}")
        print(f"Price: {result['price']}")
        print(f"Type: {result['property_type']}")
        print(f"Location: {result['city']}, {result['county']}")
        print(f"URL: {result['url']}")
        print(f"\nDescription:\n{result['description']}")

        # Display Wikipedia data if available
        if result.get('wiki_data'):
            print(f"\nWikipedia Data:")
            for wiki_entry in result['wiki_data']:
                matched_name = wiki_entry.get('matched_based_on', 'Unknown')

                # Display city data
                if 'city' in wiki_entry and wiki_entry['city']:
                    print(f"  City ({matched_name}):")
                    city_data = wiki_entry['city']
                    if 'population' in city_data:
                        print(f"    Population: {city_data['population']}")
                    if 'area_km2' in city_data:
                        print(f"    Area: {city_data['area_km2']} km²")
                    if 'elevation_m' in city_data:
                        print(f"    Elevation: {city_data['elevation_m']} m")
                    elif 'elevation_ft' in city_data:
                        print(f"    Elevation: {city_data['elevation_ft']} ft")
                    if 'founded' in city_data:
                        print(f"    Founded: {city_data['founded']}")

                # Display school district data
                if 'school_district' in wiki_entry and wiki_entry['school_district']:
                    print(f"  School District ({matched_name}):")
                    district_data = wiki_entry['school_district']
                    if 'students' in district_data:
                        print(f"    Students: {district_data['students']}")
                    if 'schools' in district_data:
                        print(f"    Schools: {district_data['schools']}")
                    if 'teachers' in district_data:
                        print(f"    Teachers: {district_data['teachers']}")

        print(f"\nTerm Metrics:")
        print(f"{'Term':<15} {'TF':<6} {'IDF':<8}")
        for term, metrics in result['term_metrics'].items():
            if metrics['present']:
                print(f"{term:<15} {metrics['tf']:<6} {metrics['idf']:<8}")

        print("\n----")


def main():
    import sys

    method = 'tfidf'
    top_k = 5
    index_file = 'data/index.jsonl'
    query_text = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--method' and i + 1 < len(args):
            method = args[i + 1]
            i += 2
        elif args[i] == '--top' and i + 1 < len(args):
            top_k = int(args[i + 1])
            i += 2
        elif args[i] == '--index' and i + 1 < len(args):
            index_file = args[i + 1]
            i += 2
        elif args[i] == '--query' and i + 1 < len(args):
            query_text = args[i + 1]
            i += 2
        else:
            # Treat remaining args as query if no --query flag
            if query_text is None:
                query_text = ' '.join(args[i:])
            break

    search = SearchEngine(index_file)

    # If query provided via command line, run once and exit
    if query_text:
        print(f"\n{'='*80}")
        print(f"SEARCHING: '{query_text}'")
        print(f"Method: {method.upper()} | Top Results: {top_k}")
        print(f"{'='*80}")

        results = search.search(query_text, method=method, top_k=top_k)
        display_results(results)
    else:
        # Interactive mode
        while True:
            try:
                query = input("Enter search query (Ctrl+C to exit): ").strip()

                if not query:
                    continue

                print(f"\n{'='*80}")
                print(f"SEARCHING: '{query}'")
                print(f"Method: {method.upper()} | Top Results: {top_k}")
                print(f"{'='*80}")

                results = search.search(query, method=method, top_k=top_k)

                display_results(results)
            except (KeyboardInterrupt, EOFError):
                print("\n\nExiting search engine. Goodbye!")
                break


if __name__ == "__main__":
    main()
