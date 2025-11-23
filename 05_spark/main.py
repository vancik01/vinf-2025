#!/usr/bin/env python3
"""
VINF Wikipedia Integration Pipeline - Main Entry Point

Runs either:
1. HTML Property Extraction (Stage 1)
2. Wikipedia Integration Pipeline (Stages 2-8)
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime
import json

from pyspark.sql import SparkSession

# Import our modules
from extract_html_properties import extract_properties_from_html
from wiki_pipeline import (
    stage2_extract_locations,
    stage3_find_wiki_pages,
    stage4_deduplicate_wiki_pages,
    stage5_extract_wiki_content,
    stage6_create_property_wiki_mapping,
    stage7_generate_jsonl_metadata,
    stage8_generate_school_district_stats
)


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(output_dir: str):
    """Setup logging to file and console"""
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'pipeline.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


# ============================================================================
# PIPELINE RUNNERS
# ============================================================================

def run_extraction_only(
    pages_dir: str = "data/scraper_data/pages",
    output_dir: str = "spark_data",
    num_partitions: int = 8,
    driver_memory: str = "8g",
    executor_memory: str = "12g"
):
    """
    Run HTML property extraction only (Stage 1)
    """
    logger = setup_logging(output_dir)

    logger.info("=" * 80)
    logger.info("HTML PROPERTY EXTRACTION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Pages directory: {pages_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Partitions: {num_partitions}")
    logger.info("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("VINF HTML Property Extraction") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.sql.shuffle.partitions", str(num_partitions)) \
        .getOrCreate()

    logger.info(f"Spark session: {spark.sparkContext.applicationId}")

    start_time = datetime.now()

    try:
        # Stage 1: Extract properties from HTML
        properties_df = extract_properties_from_html(
            spark, pages_dir, output_dir, num_partitions, logger
        )

        # Generate summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        summary = {
            'pipeline_type': 'extraction_only',
            'metadata': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'total_duration_seconds': duration,
                'spark_app_id': spark.sparkContext.applicationId
            },
            'statistics': {
                'properties_extracted': properties_df.count()
            }
        }

        summary_file = Path(output_dir) / "extraction_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info("=" * 80)
        logger.info("EXTRACTION COMPLETED SUCCESSFULLY!")
        logger.info(f"Total duration: {duration:.1f} seconds")
        logger.info(f"Summary saved to: {summary_file}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


def run_wiki_pipeline_only(
    parsed_data_path: str = "spark_data/parsed/parsed.parquet",
    wiki_index_path: str = "wiki/enwiki-20251020-pages-articles-multistream-index.txt.bz2",
    wiki_dump_path: str = "wiki/enwiki-20251020-pages-articles-multistream.xml.bz2",
    output_dir: str = "spark_data",
    num_partitions: int = 20,
    driver_memory: str = "8g",
    executor_memory: str = "12g"
):
    """
    Run Wikipedia integration pipeline only (Stages 2-8)

    Loads previously extracted property data
    """
    logger = setup_logging(output_dir)

    logger.info("=" * 80)
    logger.info("WIKIPEDIA INTEGRATION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Parsed data: {parsed_data_path}")
    logger.info(f"Wikipedia index: {wiki_index_path}")
    logger.info(f"Wikipedia dump: {wiki_dump_path}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Partitions: {num_partitions}")
    logger.info("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("VINF Wikipedia Integration Pipeline") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.sql.shuffle.partitions", str(num_partitions)) \
        .getOrCreate()

    logger.info(f"Spark session: {spark.sparkContext.applicationId}")

    start_time = datetime.now()

    try:
        # Load previously extracted properties
        logger.info(f"Loading properties from: {parsed_data_path}")
        properties_df = spark.read.parquet(parsed_data_path)
        properties_df.cache()
        logger.info(f"Loaded {properties_df.count()} properties")

        # Stage 2: Extract cities and school districts
        cities_df, school_districts_df = stage2_extract_locations(
            spark, properties_df, output_dir, logger
        )

        # Stage 3: Find Wikipedia pages in index
        found_pages_df = stage3_find_wiki_pages(
            spark, cities_df, school_districts_df, wiki_index_path,
            output_dir, num_partitions, logger
        )

        # Stage 4: Deduplicate Wikipedia pages
        deduplicated_df = stage4_deduplicate_wiki_pages(
            spark, found_pages_df, output_dir, logger
        )

        # Stage 5: Extract Wikipedia content (IMPROVED)
        extracted_wiki_df = stage5_extract_wiki_content(
            spark, deduplicated_df, wiki_dump_path,
            output_dir, num_partitions, logger
        )
        extracted_wiki_df.cache()

        # Stage 6: Create property-wiki mappings
        mapping_df = stage6_create_property_wiki_mapping(
            spark, properties_df, extracted_wiki_df, output_dir, logger
        )
        mapping_df.cache()

        # Stage 7: Generate JSONL metadata with extracted properties
        stage7_generate_jsonl_metadata(
            spark, mapping_df, output_dir, logger
        )

        # Stage 8: Generate school district statistics
        stage8_generate_school_district_stats(
            spark, mapping_df, output_dir, logger
        )

        # Generate final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        summary = {
            'pipeline_type': 'wiki_integration',
            'pipeline_metadata': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'total_duration_seconds': duration,
                'spark_app_id': spark.sparkContext.applicationId
            },
            'stage_statistics': {
                'stage2_cities': cities_df.count(),
                'stage2_districts': school_districts_df.count(),
                'stage3_found_pages': found_pages_df.count(),
                'stage4_deduplicated': deduplicated_df.count(),
                'stage5_extracted': extracted_wiki_df.count(),
                'stage6_mappings': mapping_df.count()
            }
        }

        summary_file = Path(output_dir) / "wiki_pipeline_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info("=" * 80)
        logger.info("WIKIPEDIA PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"Total duration: {duration:.1f} seconds")
        logger.info(f"Summary saved to: {summary_file}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


def run_join_wiki_data(
    parsed_data_path: str = "spark_data/parsed/parsed.parquet",
    wiki_metadata_path: str = "spark_data/property_wiki_metadata.jsonl",
    output_path: str = "spark_data/properties_with_wiki.jsonl",
    num_partitions: int = 20,
    driver_memory: str = "8g",
    executor_memory: str = "12g"
):
    """
    Join parsed property data with Wikipedia metadata
    Creates a combined JSONL file for indexing
    """
    logger = setup_logging("spark_data")

    logger.info("=" * 80)
    logger.info("JOIN WIKI DATA WITH PARSED PROPERTIES")
    logger.info("=" * 80)
    logger.info(f"Parsed data: {parsed_data_path}")
    logger.info(f"Wiki metadata: {wiki_metadata_path}")
    logger.info(f"Output: {output_path}")
    logger.info("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("VINF Join Wiki Data") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.sql.shuffle.partitions", str(num_partitions)) \
        .getOrCreate()

    logger.info(f"Spark session: {spark.sparkContext.applicationId}")

    try:
        from pyspark.sql.functions import collect_list, struct, col
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType

        # Load parsed data
        logger.info(f"Loading parsed data from: {parsed_data_path}")
        parsed_df = spark.read.parquet(parsed_data_path)

        # Rename file_hash to property_id for join compatibility
        parsed_df = parsed_df.withColumnRenamed("file_hash", "property_id")

        total_properties = parsed_df.count()
        logger.info(f"Loaded {total_properties} parsed properties")

        # Load wiki metadata with schema
        logger.info(f"Loading wiki metadata from: {wiki_metadata_path}")
        schema = StructType([
            StructField("property_id", StringType(), False),
            StructField("wiki_page_path", StringType(), True),
            StructField("match_type", StringType(), True),
            StructField("matched_based_on", StringType(), True),
            StructField("match_score", DoubleType(), True),
            StructField("match_confidence", StringType(), True),
            StructField("city", MapType(StringType(), StringType()), True),
            StructField("school_district", MapType(StringType(), StringType()), True)
        ])

        wiki_df = spark.read.schema(schema).json(wiki_metadata_path)

        # Group wiki data by property_id (handles properties with both city and school_district)
        wiki_grouped = wiki_df.groupBy("property_id").agg(
            collect_list(
                struct(
                    col("wiki_page_path"),
                    col("match_type"),
                    col("matched_based_on"),
                    col("match_score"),
                    col("match_confidence"),
                    col("city"),
                    col("school_district")
                )
            ).alias("wiki_data")
        )

        logger.info(f"Wiki metadata loaded for {wiki_grouped.count()} properties")

        # Join data (left join to keep all properties)
        logger.info("Joining parsed data with wiki metadata...")
        joined_df = parsed_df.join(wiki_grouped, on="property_id", how="left")

        # Statistics
        with_wiki = joined_df.filter(col("wiki_data").isNotNull()).count()
        logger.info(f"Properties with wiki data: {with_wiki}/{total_properties} ({with_wiki/total_properties*100:.1f}%)")

        # Write to JSONL (coalesce to single file)
        logger.info(f"Writing joined data to: {output_path}")
        temp_dir = str(output_path) + ".tmp"
        joined_df.coalesce(1).write.mode("overwrite").json(temp_dir)

        # Move the part file to final location
        import os
        import shutil
        part_files = [f for f in os.listdir(temp_dir) if f.startswith('part-') and f.endswith('.json')]
        if part_files:
            shutil.move(
                os.path.join(temp_dir, part_files[0]),
                output_path
            )
            shutil.rmtree(temp_dir)

        # Count lines
        with open(output_path, 'r') as f:
            line_count = sum(1 for _ in f)

        logger.info(f"Created JSONL file with {line_count} records: {output_path}")

        # Show sample
        logger.info("\nSample record (first 500 chars):")
        with open(output_path, 'r') as f:
            first_record = json.loads(f.readline())
            logger.info(json.dumps(first_record, indent=2)[:500] + "...")

        logger.info("=" * 80)
        logger.info("JOIN COMPLETE!")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Join failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


def run_full_pipeline(
    pages_dir: str = "data/scraper_data/pages",
    wiki_index_path: str = "wiki/enwiki-20251020-pages-articles-multistream-index.txt.bz2",
    wiki_dump_path: str = "wiki/enwiki-20251020-pages-articles-multistream.xml.bz2",
    output_dir: str = "spark_data",
    num_partitions: int = 20,
    driver_memory: str = "8g",
    executor_memory: str = "12g"
):
    """
    Run complete pipeline: Extraction + Wikipedia Integration

    All 8 stages using PySpark for distributed processing
    """
    logger = setup_logging(output_dir)

    logger.info("=" * 80)
    logger.info("FULL VINF WIKIPEDIA INTEGRATION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Pages directory: {pages_dir}")
    logger.info(f"Wikipedia index: {wiki_index_path}")
    logger.info(f"Wikipedia dump: {wiki_dump_path}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Partitions: {num_partitions}")
    logger.info("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("VINF Full Pipeline") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.sql.shuffle.partitions", str(num_partitions)) \
        .getOrCreate()

    logger.info(f"Spark session: {spark.sparkContext.applicationId}")

    start_time = datetime.now()

    try:
        # Stage 1: Parse HTML properties
        properties_df = extract_properties_from_html(
            spark, pages_dir, output_dir, num_partitions // 2, logger
        )
        properties_df.cache()

        # Stage 2: Extract cities and school districts
        cities_df, school_districts_df = stage2_extract_locations(
            spark, properties_df, output_dir, logger
        )

        # Stage 3: Find Wikipedia pages in index
        found_pages_df = stage3_find_wiki_pages(
            spark, cities_df, school_districts_df, wiki_index_path,
            output_dir, num_partitions, logger
        )

        # Stage 4: Deduplicate Wikipedia pages
        deduplicated_df = stage4_deduplicate_wiki_pages(
            spark, found_pages_df, output_dir, logger
        )

        # Stage 5: Extract Wikipedia content (IMPROVED)
        extracted_wiki_df = stage5_extract_wiki_content(
            spark, deduplicated_df, wiki_dump_path,
            output_dir, num_partitions, logger
        )
        extracted_wiki_df.cache()

        # Stage 6: Create property-wiki mappings
        mapping_df = stage6_create_property_wiki_mapping(
            spark, properties_df, extracted_wiki_df, output_dir, logger
        )
        mapping_df.cache()

        # Stage 7: Generate JSONL metadata with extracted properties
        stage7_generate_jsonl_metadata(
            spark, mapping_df, output_dir, logger
        )

        # Stage 8: Generate school district statistics
        stage8_generate_school_district_stats(
            spark, mapping_df, output_dir, logger
        )

        # Generate final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        summary = {
            'pipeline_type': 'full_pipeline',
            'pipeline_metadata': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'total_duration_seconds': duration,
                'spark_app_id': spark.sparkContext.applicationId
            },
            'stage_statistics': {
                'stage1_properties': properties_df.count(),
                'stage2_cities': cities_df.count(),
                'stage2_districts': school_districts_df.count(),
                'stage3_found_pages': found_pages_df.count(),
                'stage4_deduplicated': deduplicated_df.count(),
                'stage5_extracted': extracted_wiki_df.count(),
                'stage6_mappings': mapping_df.count()
            }
        }

        summary_file = Path(output_dir) / "pipeline_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"Total duration: {duration:.1f} seconds")
        logger.info(f"Summary saved to: {summary_file}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='VINF Wikipedia Integration Pipeline - Modular Version',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run extraction only
  python main.py extraction --pages-dir data/scraper_data/pages

  # Run Wikipedia integration only (requires extracted data)
  python main.py wiki --parsed-data spark_data/parsed/parsed.parquet

  # Run full pipeline
  python main.py full --pages-dir data/scraper_data/pages
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Pipeline mode')

    # Extraction mode
    extraction_parser = subparsers.add_parser('extraction', help='Run HTML extraction only')
    extraction_parser.add_argument('--pages-dir', default='data/scraper_data/pages',
                                    help='Directory containing HTML property files')
    extraction_parser.add_argument('--output-dir', default='spark_data',
                                    help='Base output directory')
    extraction_parser.add_argument('--partitions', type=int, default=8,
                                    help='Number of Spark partitions')
    extraction_parser.add_argument('--driver-memory', default='8g',
                                    help='Spark driver memory')
    extraction_parser.add_argument('--executor-memory', default='12g',
                                    help='Spark executor memory')

    # Wikipedia pipeline mode
    wiki_parser = subparsers.add_parser('wiki', help='Run Wikipedia integration only')
    wiki_parser.add_argument('--parsed-data', default='spark_data/parsed/parsed.parquet',
                             help='Path to parsed property data (parquet)')
    wiki_parser.add_argument('--wiki-index', default='wiki/enwiki-20251020-pages-articles-multistream-index.txt.bz2',
                             help='Wikipedia index file (bz2 compressed)')
    wiki_parser.add_argument('--wiki-dump', default='wiki/enwiki-20251020-pages-articles-multistream.xml.bz2',
                             help='Wikipedia dump file (bz2 compressed)')
    wiki_parser.add_argument('--output-dir', default='spark_data',
                             help='Base output directory')
    wiki_parser.add_argument('--partitions', type=int, default=20,
                             help='Number of Spark partitions')
    wiki_parser.add_argument('--driver-memory', default='8g',
                             help='Spark driver memory')
    wiki_parser.add_argument('--executor-memory', default='12g',
                             help='Spark executor memory')

    # Full pipeline mode
    full_parser = subparsers.add_parser('full', help='Run full pipeline (extraction + wiki)')
    full_parser.add_argument('--pages-dir', default='data/scraper_data/pages',
                             help='Directory containing HTML property files')
    full_parser.add_argument('--wiki-index', default='wiki/enwiki-20251020-pages-articles-multistream-index.txt.bz2',
                             help='Wikipedia index file (bz2 compressed)')
    full_parser.add_argument('--wiki-dump', default='wiki/enwiki-20251020-pages-articles-multistream.xml.bz2',
                             help='Wikipedia dump file (bz2 compressed)')
    full_parser.add_argument('--output-dir', default='spark_data',
                             help='Base output directory')
    full_parser.add_argument('--partitions', type=int, default=20,
                             help='Number of Spark partitions')
    full_parser.add_argument('--driver-memory', default='8g',
                             help='Spark driver memory')
    full_parser.add_argument('--executor-memory', default='12g',
                             help='Spark executor memory')

    # Join mode
    join_parser = subparsers.add_parser('join', help='Join parsed data with wiki metadata')
    join_parser.add_argument('--parsed-data', default='spark_data/parsed/parsed.parquet',
                             help='Path to parsed property data (parquet)')
    join_parser.add_argument('--wiki-metadata', default='spark_data/property_wiki_metadata.jsonl',
                             help='Path to wiki metadata (jsonl)')
    join_parser.add_argument('--output', default='spark_data/properties_with_wiki.jsonl',
                             help='Output JSONL file path')
    join_parser.add_argument('--partitions', type=int, default=20,
                             help='Number of Spark partitions')
    join_parser.add_argument('--driver-memory', default='8g',
                             help='Spark driver memory')
    join_parser.add_argument('--executor-memory', default='12g',
                             help='Spark executor memory')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Run selected pipeline
    if args.command == 'extraction':
        run_extraction_only(
            pages_dir=args.pages_dir,
            output_dir=args.output_dir,
            num_partitions=args.partitions,
            driver_memory=args.driver_memory,
            executor_memory=args.executor_memory
        )
    elif args.command == 'wiki':
        run_wiki_pipeline_only(
            parsed_data_path=args.parsed_data,
            wiki_index_path=args.wiki_index,
            wiki_dump_path=args.wiki_dump,
            output_dir=args.output_dir,
            num_partitions=args.partitions,
            driver_memory=args.driver_memory,
            executor_memory=args.executor_memory
        )
    elif args.command == 'full':
        run_full_pipeline(
            pages_dir=args.pages_dir,
            wiki_index_path=args.wiki_index,
            wiki_dump_path=args.wiki_dump,
            output_dir=args.output_dir,
            num_partitions=args.partitions,
            driver_memory=args.driver_memory,
            executor_memory=args.executor_memory
        )
    elif args.command == 'join':
        run_join_wiki_data(
            parsed_data_path=args.parsed_data,
            wiki_metadata_path=args.wiki_metadata,
            output_path=args.output,
            num_partitions=args.partitions,
            driver_memory=args.driver_memory,
            executor_memory=args.executor_memory
        )


if __name__ == "__main__":
    main()
