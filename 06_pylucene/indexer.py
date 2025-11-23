import lucene
import json
import time
from pathlib import Path

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import (
    Document, Field, StringField, TextField,
    NumericDocValuesField, StoredField, FieldType, LongPoint, IntPoint
)
from org.apache.lucene.index import (
    IndexWriter, IndexWriterConfig, IndexOptions
)
from org.apache.lucene.store import FSDirectory

class RealEstateIndexer:
    def __init__(self, index_dir):
        if not lucene.getVMEnv():
            lucene.initVM()

        self.index_dir = index_dir
        self.analyzer = StandardAnalyzer()

        self.text_field_type = FieldType()
        self.text_field_type.setStored(True)
        self.text_field_type.setTokenized(True)
        self.text_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
        self.text_field_type.setStoreTermVectors(True)
        self.text_field_type.setStoreTermVectorPositions(True)

    def create_document(self, prop):
        doc = Document()

        prop_id = prop.get('property_id', '')
        doc.add(StringField('property_id', prop_id, Field.Store.YES))

        title = prop.get('title', '')
        if title:
            doc.add(TextField('title', title, Field.Store.YES))

        description = prop.get('description', '')
        if description:
            doc.add(Field('description', description, self.text_field_type))

        price_str = prop.get('price', '').replace('$', '').replace(',', '')
        if price_str:
            try:
                price = int(price_str)
                doc.add(LongPoint('price', price))
                doc.add(NumericDocValuesField('price', price))
                doc.add(StoredField('price', price))
            except:
                pass

        beds = prop.get('beds')
        if beds:
            try:
                beds_int = int(beds)
                doc.add(IntPoint('beds', beds_int))
                doc.add(NumericDocValuesField('beds', beds_int))
                doc.add(StoredField('beds', beds_int))
            except:
                pass

        baths = prop.get('baths')
        if baths:
            try:
                baths_int = int(baths)
                doc.add(IntPoint('baths', baths_int))
                doc.add(NumericDocValuesField('baths', baths_int))
                doc.add(StoredField('baths', baths_int))
            except:
                pass

        sqft = prop.get('sqft')
        if sqft:
            try:
                sqft_int = int(sqft)
                doc.add(IntPoint('sqft', sqft_int))
                doc.add(NumericDocValuesField('sqft', sqft_int))
                doc.add(StoredField('sqft', sqft_int))
            except:
                pass

        city = prop.get('city', '')
        if city:
            doc.add(StringField('city_exact', city, Field.Store.YES))
            doc.add(TextField('city', city, Field.Store.NO))

        county = prop.get('county', '')
        if county:
            doc.add(StringField('county', county, Field.Store.YES))

        title_parts = title.split()
        state = ''
        for i, part in enumerate(title_parts):
            if len(part) == 2 and part.isupper() and i > 0:
                state = part
                break
        if state:
            doc.add(StringField('state', state, Field.Store.YES))

        school_district = prop.get('school_district', '')
        if school_district:
            doc.add(StringField('school_district_exact', school_district, Field.Store.YES))
            doc.add(TextField('school_district', school_district, Field.Store.NO))

        prop_type = prop.get('property_type', '')
        if prop_type:
            doc.add(StringField('property_type', prop_type, Field.Store.YES))

        wiki_data = prop.get('wiki_data', [])
        if wiki_data:
            for wiki_entry in wiki_data:
                city_data = wiki_entry.get('city', {})
                if city_data:
                    population = city_data.get('population', '').replace(',', '')
                    if population:
                        try:
                            pop_int = int(population)
                            doc.add(LongPoint('population', pop_int))
                            doc.add(NumericDocValuesField('population', pop_int))
                            doc.add(StoredField('population', pop_int))
                        except:
                            pass

                    area = city_data.get('area_km2', '').replace(',', '')
                    if area:
                        try:
                            area_float = float(area)
                            area_int = int(area_float * 100)
                            doc.add(NumericDocValuesField('area_km2', area_int))
                            doc.add(StoredField('area_km2', area_int))
                        except:
                            pass

                    elevation = city_data.get('elevation_ft', '').replace(',', '')
                    if elevation:
                        try:
                            elev_int = int(float(elevation))
                            doc.add(NumericDocValuesField('elevation_ft', elev_int))
                            doc.add(StoredField('elevation_ft', elev_int))
                        except:
                            pass

                district_data = wiki_entry.get('school_district', {})
                if district_data:
                    students = district_data.get('students', '').replace(',', '')
                    if students:
                        try:
                            students_int = int(students)
                            doc.add(LongPoint('students', students_int))
                            doc.add(NumericDocValuesField('students', students_int))
                            doc.add(StoredField('students', students_int))
                        except:
                            pass

                    schools = district_data.get('schools', '').replace(',', '')
                    if schools:
                        try:
                            schools_int = int(schools)
                            doc.add(NumericDocValuesField('schools', schools_int))
                            doc.add(StoredField('schools', schools_int))
                        except:
                            pass

        return doc

    def build_index(self, jsonl_file, wiki_only=False):
        print(f"Building PyLucene index from {jsonl_file}...")
        print(f"Index directory: {self.index_dir}")
        print(f"Wiki-enriched only: {wiki_only}")

        Path(self.index_dir).mkdir(parents=True, exist_ok=True)

        directory = FSDirectory.open(Paths.get(self.index_dir))
        config = IndexWriterConfig(self.analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer = IndexWriter(directory, config)

        start_time = time.time()
        doc_count = 0
        wiki_count = 0

        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                prop = json.loads(line)

                if wiki_only and 'wiki_data' not in prop:
                    continue

                doc = self.create_document(prop)
                writer.addDocument(doc)
                doc_count += 1

                if 'wiki_data' in prop:
                    wiki_count += 1

                if doc_count % 1000 == 0:
                    print(f"  Indexed {doc_count} documents...")

        writer.commit()
        writer.close()

        elapsed = time.time() - start_time

        print(f"\n{'='*80}")
        print(f"INDEXING COMPLETE!")
        print(f"{'='*80}")
        print(f"Total documents indexed: {doc_count}")
        print(f"Documents with wiki data: {wiki_count} ({wiki_count/doc_count*100:.1f}%)")
        print(f"Time elapsed: {elapsed:.2f} seconds")
        print(f"Index location: {self.index_dir}")
        print(f"{'='*80}")

def main():
    import sys

    base_dir = Path(__file__).parent
    data_file = base_dir.parent / "data" / "properties_with_wiki.jsonl"
    index_dir = str(base_dir / "index")

    if len(sys.argv) >= 2:
        data_file = sys.argv[1]
    if len(sys.argv) >= 3:
        index_dir = sys.argv[2]

    wiki_only = 'wiki' in str(data_file)

    indexer = RealEstateIndexer(index_dir)
    indexer.build_index(data_file, wiki_only=wiki_only)

if __name__ == "__main__":
    main()
