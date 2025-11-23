import lucene
import sys
import re
from pathlib import Path

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import (
    IndexSearcher, BooleanQuery, BooleanClause,
    TermQuery, PhraseQuery, FuzzyQuery,
    TermRangeQuery, Query
)
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.document import LongPoint, IntPoint
from org.apache.lucene.index import Term

class PyLuceneSearchEngine:
    def __init__(self, index_dir):
        if not lucene.getVMEnv():
            lucene.initVM()

        self.index_dir = index_dir
        self.analyzer = StandardAnalyzer()

        directory = FSDirectory.open(Paths.get(index_dir))
        self.reader = DirectoryReader.open(directory)
        self.searcher = IndexSearcher(self.reader)

        print(f"Index loaded: {self.reader.numDocs()} documents")

    def parse_numeric_range(self, query_str):
        long_fields = {'price', 'population', 'students'}
        int_fields = {'beds', 'baths', 'sqft'}
        all_numeric_fields = long_fields | int_fields

        numeric_queries = []
        remaining_query = query_str

        range_pattern = r'(\w+):\[(\d+|\*)\s+TO\s+(\d+|\*)\]'
        for match in re.finditer(range_pattern, query_str):
            field, min_val, max_val = match.groups()
            if field in all_numeric_fields:
                min_bound = 0 if min_val == '*' else int(min_val)
                max_bound = 9999999999 if max_val == '*' else int(max_val)

                if field in long_fields:
                    q = LongPoint.newRangeQuery(field, min_bound, max_bound)
                else:
                    q = IntPoint.newRangeQuery(field, min_bound, max_bound)

                numeric_queries.append(q)
                remaining_query = remaining_query.replace(match.group(0), '__REMOVED__')

        exact_pattern = r'(\w+):(\d+)'
        for match in re.finditer(exact_pattern, remaining_query):
            field, value = match.groups()
            if field in all_numeric_fields:
                val = int(value)

                if field in long_fields:
                    q = LongPoint.newExactQuery(field, val)
                else:
                    q = IntPoint.newExactQuery(field, val)

                numeric_queries.append(q)
                remaining_query = remaining_query.replace(match.group(0), '__REMOVED__')

        remaining_query = remaining_query.replace('__REMOVED__', '')
        remaining_query = re.sub(r'\s+(AND|OR|NOT)\s+(AND|OR|NOT)\s+', r' \1 ', remaining_query)
        remaining_query = re.sub(r'^\s*(AND|OR|NOT)\s+', '', remaining_query)
        remaining_query = re.sub(r'\s+(AND|OR|NOT)\s*$', '', remaining_query)
        remaining_query = re.sub(r'\s+', ' ', remaining_query).strip()

        return numeric_queries, remaining_query

    def parse_query(self, query_str, default_field='description'):
        numeric_queries, text_query = self.parse_numeric_range(query_str)

        if numeric_queries:
            builder = BooleanQuery.Builder()

            for num_q in numeric_queries:
                builder.add(num_q, BooleanClause.Occur.MUST)

            text_query = text_query.strip()
            text_query = re.sub(r'^(AND|OR|NOT)\s+', '', text_query)
            text_query = re.sub(r'\s+(AND|OR|NOT)$', '', text_query)
            text_query = re.sub(r'\s+', ' ', text_query).strip()

            if text_query and text_query not in ['AND', 'OR', 'NOT', '']:
                parser = QueryParser(default_field, self.analyzer)
                parser.setAllowLeadingWildcard(True)
                text_parsed = parser.parse(text_query)
                builder.add(text_parsed, BooleanClause.Occur.MUST)

            return builder.build()
        else:
            parser = QueryParser(default_field, self.analyzer)
            parser.setAllowLeadingWildcard(True)
            return parser.parse(query_str)

    def search(self, query_str, max_results=10):
        query = self.parse_query(query_str)

        print(f"\nQuery: {query_str}")
        print(f"Parsed: {query}")

        hits = self.searcher.search(query, max_results)

        print(f"Found {hits.totalHits.value} matches\n")

        results = []
        for i, score_doc in enumerate(hits.scoreDocs[:max_results], 1):
            doc = self.searcher.doc(score_doc.doc)
            score = score_doc.score

            result = {
                'rank': i,
                'score': score,
                'property_id': doc.get('property_id'),
                'title': doc.get('title'),
                'price': doc.get('price'),
                'beds': doc.get('beds'),
                'baths': doc.get('baths'),
                'sqft': doc.get('sqft'),
                'property_type': doc.get('property_type'),
                'city': doc.get('city_exact'),
                'city_exact': doc.get('city_exact'),
                'county': doc.get('county'),
                'state': doc.get('state'),
                'school_district': doc.get('school_district_exact'),
                'school_district_exact': doc.get('school_district_exact'),
                'description': doc.get('description'),
                'population': doc.get('population'),
                'area_km2': doc.get('area_km2'),
                'elevation_ft': doc.get('elevation_ft'),
                'students': doc.get('students'),
                'schools': doc.get('schools')
            }

            results.append(result)

        return results

    def display_results(self, results):
        for result in results:
            print(f"\nRESULT #{result['rank']} | Score: {result['score']:.6f} (BM25)")
            print(f"\nTitle: {result['title']}")

            price = result.get('price', 'N/A')
            if price and price != 'N/A':
                try:
                    print(f"Price: ${int(price):,}")
                except:
                    print(f"Price: {price}")
            else:
                print(f"Price: N/A")

            prop_type = result.get('property_type', 'N/A')
            print(f"Type: {prop_type}")

            city = result.get('city_exact', 'N/A')
            county = result.get('county', 'N/A')
            print(f"Location: {city}, {county}")

            print(f"URL: N/A")

            desc = result.get('description', '')
            if desc:
                words = desc.split()
                preview = ' '.join(words[:20])
                if len(words) > 20:
                    preview += '...'
                print(f"\nDescription:\n{preview}")
            else:
                print(f"\nDescription:\nN/A")

            if result.get('population') or result.get('students'):
                print(f"\nWikipedia Data:")

                if result.get('population') or result.get('area_km2'):
                    city_name = result.get('city_exact', 'City')
                    print(f"  City ({city_name}):")

                    if result.get('population'):
                        try:
                            pop = int(result['population'])
                            print(f"    Population: {pop:,}")
                        except:
                            print(f"    Population: {result['population']}")

                    if result.get('area_km2'):
                        try:
                            area = int(result['area_km2']) / 100
                            print(f"    Area: {area:.2f} km²")
                        except:
                            print(f"    Area: {result['area_km2']}")

                    if result.get('elevation_ft'):
                        print(f"    Elevation: {result['elevation_ft']} ft")

                if result.get('students') or result.get('schools'):
                    district_name = result.get('school_district_exact', 'School District')
                    print(f"  School District ({district_name}):")

                    if result.get('students'):
                        try:
                            students = int(result['students'])
                            print(f"    Students: {students:,}")
                        except:
                            print(f"    Students: {result['students']}")

                    if result.get('schools'):
                        print(f"    Schools: {result['schools']}")

            print("\n----")

    def demo_queries(self):
        queries = [
            ("luxury pool", "Simple text search (OR)"),
            ("luxury AND pool", "Boolean AND - both terms required"),
            ("Texas OR California", "Boolean OR - either term"),
            ("house NOT apartment", "Boolean NOT - exclude term"),
            ('"luxury home"', "Phrase query - exact phrase"),
            ("Dalles~", "Fuzzy query - edit distance (Dallas misspelled)"),
            ("house*", "Wildcard query - starts with 'house'"),
            ("city_exact:Dallas", "Field search - exact city match"),
            ("price:[500000 TO 1000000]", "Range query - price $500k-$1M"),
            ("city_exact:Austin AND price:[300000 TO 600000]", "Complex - city + price range"),
            ("population:[1000000 TO *]", "Wiki-enriched - cities >1M population"),
            ("(luxury OR modern) AND pool AND price:[* TO 500000]", "Complex boolean - multiple conditions")
        ]

        print("\n" + "="*80)
        print("PYLUCENE QUERY TYPE DEMONSTRATIONS")
        print("="*80)

        for i, (query, description) in enumerate(queries, 1):
            print(f"\n{'='*80}")
            print(f"DEMO {i}: {description}")
            print(f"{'='*80}")

            try:
                results = self.search(query, max_results=3)
                if results:
                    self.display_results(results[:2])
                else:
                    print("No results found.\n")
            except Exception as e:
                print(f"Error: {e}\n")

            input("Press Enter to continue...")

    def interactive(self):
        print("\n" + "="*80)
        print("INTERACTIVE SEARCH MODE")
        print("="*80)
        print("\nQuery syntax examples:")
        print("  Simple text:       luxury pool")
        print("  Boolean:           luxury AND pool")
        print("  Phrase:            \"luxury home\"")
        print("  Fuzzy:             Dallas~")
        print("  Range:             price:[100000 TO 500000]")
        print("  Field:             city_exact:Austin")
        print("  Complex:           city_exact:Dallas AND price:[* TO 500000]")
        print("\nType 'quit' to exit\n")

        while True:
            try:
                query = input("\nEnter query: ").strip()

                if query.lower() in ['quit', 'exit', 'q']:
                    print("Goodbye!")
                    break

                if not query:
                    continue

                results = self.search(query)
                self.display_results(results)

            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")

    def close(self):
        self.reader.close()

def main():
    index_dir = Path(__file__).parent / "index"
    mode = "interactive"

    if len(sys.argv) >= 2:
        index_dir = sys.argv[1]
    if len(sys.argv) >= 3:
        mode = sys.argv[2]

    engine = PyLuceneSearchEngine(str(index_dir))

    if mode == "demo":
        engine.demo_queries()
    elif mode == "interactive":
        engine.interactive()
    elif mode.startswith("query:"):
        query = mode.replace("query:", "")
        results = engine.search(query)
        engine.display_results(results)

    engine.close()

if __name__ == "__main__":
    main()
