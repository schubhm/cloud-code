import boto3
import gzip
import io
import os
import sys
import argparse
import glob
import re
from botocore.exceptions import ClientError
from datetime import datetime
from urllib.parse import urlparse

class S3FileProcessor:
    def __init__(self, aws_access_key=None, aws_secret_key=None, region='us-east-1'):
        """
        Initialize S3 client
        If credentials not provided, will use default AWS credential chain
        """
        if aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
        else:
            self.s3_client = boto3.client('s3', region_name=region)
    
    def parse_s3_uri(self, s3_uri):
        """Parse S3 URI and return bucket and key/prefix"""
        try:
            if not s3_uri.startswith('s3://'):
                raise ValueError("URI must start with s3://")
            
            parsed = urlparse(s3_uri)
            bucket = parsed.netloc
            key_or_prefix = parsed.path.lstrip('/')
            
            return bucket, key_or_prefix
        except Exception as e:
            raise ValueError(f"Invalid S3 URI format: {e}")
    
    def list_files_in_path(self, bucket_name, prefix):
        """List all files in the S3 path"""
        try:
            print(f"🔍 Scanning for files in: s3://{bucket_name}/{prefix}")
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            files = []
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Only include actual files (not just prefixes/folders)
                        if not obj['Key'].endswith('/'):
                            files.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'modified': obj['LastModified']
                            })
            
            return files
        except Exception as e:
            raise Exception(f"Failed to list files: {e}")
    
    def process_file(self, bucket_name, file_key, country_to_keep='US', save_local_files=False, min_data_lines=1):
        """
        Main function to process S3 file:
        1. Download file
        2. Keep only rows with specified country
        3. Optionally save local before/after files for analysis
        4. Gzip and upload back (only if enough data remains)
        """
        try:
            print(f"\n{'='*80}")
            print(f"🔄 Processing: s3://{bucket_name}/{file_key}")
            print(f"{'='*80}")
            
            # Step 1: Download file
            print("1. 📥 Downloading file from S3...")
            file_content = self._download_file(bucket_name, file_key)
            
            # Step 2: Save local "before" file (optional)
            before_file = None
            after_file = None
            if save_local_files:
                print("2. 💾 Saving original file locally for analysis...")
                before_file = self._save_local_file(file_content, file_key, "before")
            
            # Step 3: Filter content (keep only specified country)
            step_num = 3 if save_local_files else 2
            print(f"{step_num}. 🔧 Filtering for ClientGeoCountry: {country_to_keep}...")
            filtered_content, data_lines_kept = self._filter_content(file_content, country_to_keep)
            
            # Step 4: Save local "after" file (optional)
            if save_local_files:
                step_num += 1
                print(f"{step_num}. 💾 Saving filtered file locally for analysis...")
                after_file = self._save_local_file(filtered_content, file_key, "after")
                # Create summary file comparing before and after
                step_num += 1
                print(f"{step_num}. 📊 Creating analysis summary...")
                self._create_summary_file(before_file, after_file, file_key, country_to_keep)
            
            # Safety check: Don't upload if insufficient data remains
            step_num += 1
            if data_lines_kept < min_data_lines:
                print(f"\n⚠️  WARNING: Only {data_lines_kept} data lines remain after filtering.")
                print(f"   Minimum required: {min_data_lines}")
                print(f"   🚫 SKIPPING upload to prevent data loss.")
                print(f"   📁 Original file preserved at: s3://{bucket_name}/{file_key}")
                
                if save_local_files and before_file and after_file:
                    print(f"\n📁 Local analysis files created:")
                    print(f"   - Original: {before_file}")
                    print(f"   - Filtered: {after_file}")
                return False
            
            # Gzip content
            print(f"{step_num}. 🗜️  Compressing filtered content...")
            compressed_content = self._gzip_content(filtered_content)
            
            # Upload back to S3 (replacing original)
            step_num += 1
            print(f"{step_num}. 📤 Uploading processed file back to S3...")
            self._upload_file(bucket_name, file_key, compressed_content)
            
            print(f"✅ SUCCESS: File processed and replaced in S3!")
            
            if save_local_files and before_file and after_file:
                print(f"\n📁 Local analysis files created:")
                print(f"   - Original: {before_file}")
                print(f"   - Filtered: {after_file}")
            
            return True
            
        except Exception as e:
            print(f"❌ ERROR processing file: {str(e)}")
            return False
    
    def process_all_files_in_path(self, s3_uri, country_to_keep='US', save_local_files=False, dry_run=False, min_data_lines=1):
        """Process all files in the given S3 path"""
        try:
            # Parse S3 URI
            bucket_name, prefix = self.parse_s3_uri(s3_uri)
            
            # List all files
            files = self.list_files_in_path(bucket_name, prefix)
            
            if not files:
                print(f"❌ No files found in: {s3_uri}")
                return
            
            print(f"📁 Found {len(files)} files to process")
            
            # Show files and ask for confirmation
            print(f"\n📋 Files to be processed:")
            total_size = 0
            for i, file_info in enumerate(files, 1):
                size_mb = file_info['size'] / 1024 / 1024
                total_size += file_info['size']
                print(f"   {i:3d}. {file_info['key']} ({size_mb:.2f} MB)")
            
            total_size_mb = total_size / 1024 / 1024
            print(f"\n📊 Total: {len(files)} files, {total_size_mb:.2f} MB")
            
            if dry_run:
                print(f"\n🔍 DRY RUN MODE - No files will be modified")
                return
            
            # Ask for confirmation
            print(f"\n⚠️  This will:")
            print(f"   - Filter each file to keep only '{country_to_keep}' entries")
            print(f"   - Replace the original files in S3")
            print(f"   - Create local analysis files")
            
            response = input(f"\n❓ Do you want to proceed? (yes/no): ").strip().lower()
            if response not in ['yes', 'y']:
                print("❌ Operation cancelled by user")
                return
            
            # Process each file
            results = []
            successful = 0
            failed = 0
            skipped = 0
            
            print(f"\n🚀 Starting batch processing...")
            
            for i, file_info in enumerate(files, 1):
                print(f"\n📄 Processing file {i}/{len(files)}")
                
                try:
                    success = self.process_file(
                        bucket_name, 
                        file_info['key'], 
                        country_to_keep, 
                        save_local_files,
                        min_data_lines
                    )
                    
                    if success:
                        successful += 1
                        results.append((file_info['key'], "✅ SUCCESS"))
                    else:
                        skipped += 1
                        results.append((file_info['key'], "⚠️  SKIPPED (no target data)"))
                        
                except Exception as e:
                    failed += 1
                    results.append((file_info['key'], f"❌ FAILED: {str(e)[:100]}"))
                    print(f"❌ Failed: {e}")
            
            # Final summary
            print(f"\n{'='*80}")
            print(f"🎯 BATCH PROCESSING COMPLETE")
            print(f"{'='*80}")
            print(f"📊 Results: {successful} successful, {skipped} skipped, {failed} failed")
            print(f"\n📋 Detailed Results:")
            
            for file_key, status in results:
                print(f"   {status}: {file_key}")
            
            if save_local_files:
                print(f"\n📁 Local analysis files saved to: ./s3_analysis_files/")
            else:
                print(f"\n📊 Processing completed without local file creation")
            
        except Exception as e:
            print(f"❌ Batch processing failed: {e}")
    
    def _download_file(self, bucket_name, file_key):
        """Download file from S3 and return content"""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read()
            
            # If file is already gzipped, decompress it
            if file_key.endswith('.gz'):
                content = gzip.decompress(content)
            
            return content.decode('utf-8')
            
        except ClientError as e:
            raise Exception(f"Failed to download file: {e}")
    
    def _filter_content(self, content, country_to_keep):
        """Keep only lines containing the specified country"""
        lines = content.split('\n')
        filtered_lines = []
        
        kept_count = 0
        removed_count = 0
        total_data_lines = 0
        
        # Define country mappings (both codes and full names)
        us_variations = [
            "US", "USA", "United States", "United States of America"
        ]
        
        for line in lines:
            # Skip completely empty lines
            if line.strip() == '':
                filtered_lines.append(line)  # Keep empty lines for structure
                continue
                
            total_data_lines += 1
            
            # Check for target country - handle both codes and full names
            is_target_country = False
            
            if country_to_keep.upper() == "US":
                # If looking for US, check all US variations
                for us_variant in us_variations:
                    if (f'"ClientGeoCountry": "{us_variant}"' in line or
                        f'"ClientGeoCountry":"{us_variant}"' in line or
                        f'ClientGeoCountry: "{us_variant}"' in line or
                        f'ClientGeoCountry: {us_variant}' in line):
                        is_target_country = True
                        break
            else:
                # For other countries, use exact match
                is_target_country = (
                    f'"ClientGeoCountry": "{country_to_keep}"' in line or
                    f'"ClientGeoCountry":"{country_to_keep}"' in line or
                    f'ClientGeoCountry: "{country_to_keep}"' in line or
                    f'ClientGeoCountry: {country_to_keep}' in line
                )
            
            if is_target_country:
                filtered_lines.append(line)
                kept_count += 1
            else:
                removed_count += 1
        
        print(f"   📊 Total data lines processed: {total_data_lines}")
        if country_to_keep.upper() == "US":
            print(f"   ✅ Kept {kept_count} lines with US data (US/USA/United States)")
        else:
            print(f"   ✅ Kept {kept_count} lines with ClientGeoCountry: {country_to_keep}")
        print(f"   ❌ Removed {removed_count} lines with other countries")
        
        return '\n'.join(filtered_lines), kept_count
    
    def _gzip_content(self, content):
        """Compress content using gzip with proper headers"""
        try:
            content_bytes = content.encode('utf-8')
            
            # Use BytesIO buffer for in-memory compression
            buffer = io.BytesIO()
            
            # Create gzip file with proper headers and compression level
            with gzip.GzipFile(
                fileobj=buffer, 
                mode='wb', 
                compresslevel=6,  # Good balance of speed and compression
                mtime=0  # Set consistent timestamp
            ) as gz_file:
                gz_file.write(content_bytes)
            
            compressed_data = buffer.getvalue()
            
            # Verify the compression worked
            original_size = len(content_bytes)
            compressed_size = len(compressed_data)
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            print(f"   📊 Compression: {original_size:,} -> {compressed_size:,} bytes ({compression_ratio:.1f}% reduction)")
            
            # Test decompression to ensure integrity
            try:
                test_decompress = gzip.decompress(compressed_data)
                if test_decompress != content_bytes:
                    raise Exception("Compression integrity check failed")
                print(f"   ✅ Compression integrity verified")
            except Exception as e:
                raise Exception(f"Compression validation failed: {e}")
            
            return compressed_data
            
        except Exception as e:
            raise Exception(f"Failed to compress content: {e}")
    
    def _save_local_file(self, content, original_file_key, stage):
        """Save content to local file for inspection"""
        # Create local directory if it doesn't exist
        local_dir = "s3_analysis_files"
        os.makedirs(local_dir, exist_ok=True)
        
        # Generate filename based on original S3 key
        base_filename = os.path.basename(original_file_key).replace('.gz', '')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_filename = f"{local_dir}/{base_filename}_{stage}_{timestamp}.txt"
        
        # Save content
        with open(local_filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # Get file stats
        file_size = os.path.getsize(local_filename)
        line_count = len(content.split('\n'))
        
        print(f"   📁 Saved {stage} file: {local_filename}")
        print(f"   📊 Size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB), Lines: {line_count:,}")
        
        return local_filename
    
    def _create_summary_file(self, before_file, after_file, original_file_key, country_to_keep):
        """Create a summary file comparing before and after"""
        local_dir = "s3_analysis_files"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = os.path.basename(original_file_key).replace('.gz', '')
        summary_filename = f"{local_dir}/{base_filename}_summary_{timestamp}.txt"
        
        # Get file stats
        before_size = os.path.getsize(before_file) if os.path.exists(before_file) else 0
        after_size = os.path.getsize(after_file) if os.path.exists(after_file) else 0
        
        with open(before_file, 'r', encoding='utf-8') as f:
            before_lines = len(f.readlines())
        
        with open(after_file, 'r', encoding='utf-8') as f:
            after_lines = len(f.readlines())
        
        # Analyze countries in the original file
        countries_analysis = self._analyze_countries_in_file(before_file)
        
        # Create summary
        summary_content = f"""S3 File Processing Analysis Summary
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

ORIGINAL S3 FILE: {original_file_key}
FILTER APPLIED: Keep only ClientGeoCountry: "{country_to_keep}"

═══════════════════════════════════════════════════════════════

BEFORE PROCESSING:
- Size: {before_size:,} bytes ({before_size/1024/1024:.2f} MB)
- Lines: {before_lines:,}

AFTER PROCESSING:
- Size: {after_size:,} bytes ({after_size/1024/1024:.2f} MB)
- Lines: {after_lines:,}

ANALYSIS RESULTS:
- Size reduction: {before_size - after_size:,} bytes ({((before_size - after_size)/before_size*100) if before_size > 0 else 0:.1f}%)
- Lines removed: {before_lines - after_lines:,}
- Lines kept: {after_lines:,} ({(after_lines/before_lines*100) if before_lines > 0 else 0:.1f}%)

COUNTRIES FOUND IN ORIGINAL FILE:
{countries_analysis}

═══════════════════════════════════════════════════════════════
"""
        
        with open(summary_filename, 'w', encoding='utf-8') as f:
            f.write(summary_content)
        
        print(f"   📋 Created summary: {summary_filename}")
        return summary_filename
    
    def _analyze_countries_in_file(self, file_path):
        """Analyze what countries are in a local file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            countries = {}
            total_data_lines = 0
            
            for line in lines:
                if line.strip() == '':
                    continue
                    
                total_data_lines += 1
                
                # Extract country using regex
                country_match = re.search(r'"ClientGeoCountry":\s*"([^"]+)"', line)
                if country_match:
                    country = country_match.group(1)
                    countries[country] = countries.get(country, 0) + 1
                else:
                    countries['UNKNOWN_FORMAT'] = countries.get('UNKNOWN_FORMAT', 0) + 1
            
            # Format results
            if not countries:
                return "No country data found"
            
            result = f"Total data lines: {total_data_lines}\n"
            for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_data_lines * 100) if total_data_lines > 0 else 0
                result += f"- {country}: {count} lines ({percentage:.1f}%)\n"
            
            return result
            
        except Exception as e:
            return f"Error analyzing countries: {e}"

    def _upload_file(self, bucket_name, file_key, content):
        """Upload compressed content back to S3, replacing the original"""
        try:
            # Verify the content is properly gzipped
            try:
                # Test decompression to ensure it's valid gzip
                test_decompress = gzip.decompress(content)
                print(f"   ✅ Gzip validation passed ({len(content)} compressed bytes -> {len(test_decompress)} uncompressed)")
            except Exception as e:
                raise Exception(f"Invalid gzip content: {e}")
            
            # Upload with minimal metadata - same as original file would have
            upload_params = {
                'Bucket': bucket_name,
                'Key': file_key,
                'Body': content,
                'ContentType': 'application/x-gzip',  # Standard gzip MIME type
                # Don't set ContentEncoding for .gz files - let them be treated as binary
            }
            
            # Perform the upload
            self.s3_client.put_object(**upload_params)
            
            print(f"   ✅ Successfully replaced: s3://{bucket_name}/{file_key}")
            
            # Verify the uploaded file can be downloaded and decompressed
            try:
                print(f"   🔍 Verifying uploaded file integrity...")
                test_response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
                test_content = test_response['Body'].read()
                test_decompressed = gzip.decompress(test_content)
                
                uploaded_size = len(test_content)
                decompressed_size = len(test_decompressed)
                
                print(f"   📊 Uploaded: {uploaded_size:,} bytes")
                print(f"   📊 Decompresses to: {decompressed_size:,} bytes")
                print(f"   ✅ Verification passed - file can be properly decompressed")
                
                # Test with a few lines to ensure it's readable text
                text_content = test_decompressed.decode('utf-8')
                lines = text_content.split('\n')[:3]
                if lines:
                    print(f"   📝 Sample content verified (first line): {lines[0][:100]}...")
                
            except Exception as e:
                print(f"   ⚠️  Warning: Could not verify uploaded file: {e}")
                raise Exception(f"Upload verification failed: {e}")
            
        except ClientError as e:
            raise Exception(f"Failed to upload file: {e}")


def main():
    """Main function with command line argument support"""
    parser = argparse.ArgumentParser(
        description='Process S3 files to filter by country',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all files in a specific date path
  python s3cleanup.py s3://super-log-qa/vader/2025/06/02/

  # Process with different country
  python s3cleanup.py s3://super-log-qa/vader/ --country "Canada"

  # Dry run to see what would be processed
  python s3cleanup.py s3://super-log-qa/vader/2025/06/ --dry-run

  # Process single file
  python s3cleanup.py s3://super-log-qa/vader/2025/06/02/file.gz
        """
    )
    
    parser.add_argument('s3_uri', help='S3 URI (s3://bucket/path/)')
    parser.add_argument('--country', default='US', help='Country to keep (default: US)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without making changes')
    parser.add_argument('--save-local', action='store_true', help='Save local analysis files (default: False)')
    parser.add_argument('--min-lines', type=int, default=1, help='Minimum data lines required to upload (default: 1)')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = S3FileProcessor()
    
    print("🚀 S3 File Processor - Batch Country Filter")
    print(f"📍 Target: {args.s3_uri}")
    print(f"🌍 Country to keep: {args.country}")
    if args.dry_run:
        print("🔍 Mode: DRY RUN (no changes will be made)")
    if args.save_local:
        print("📁 Local analysis files: ENABLED")
    else:
        print("📁 Local analysis files: DISABLED")
    print()
    
    try:
        processor.process_all_files_in_path(
            s3_uri=args.s3_uri,
            country_to_keep=args.country,
            save_local_files=args.save_local,
            dry_run=args.dry_run,
            min_data_lines=args.min_lines
        )
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def test_file_integrity():
    """Test if a specific S3 file can be downloaded and decompressed properly"""
    parser = argparse.ArgumentParser(description='Test S3 file integrity')
    parser.add_argument('s3_uri', help='S3 URI of file to test (s3://bucket/path/file.gz)')
    
    args = parser.parse_args()
    
    processor = S3FileProcessor()
    
    try:
        bucket_name, file_key = processor.parse_s3_uri(args.s3_uri)
        
        print(f"🔍 Testing file integrity: {args.s3_uri}")
        
        # Download file
        print("1. 📥 Downloading file...")
        response = processor.s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read()
        
        print(f"   📊 Downloaded {len(content):,} bytes")
        print(f"   📋 Content-Type: {response.get('ContentType', 'Unknown')}")
        print(f"   📋 Content-Encoding: {response.get('ContentEncoding', 'Unknown')}")
        
        # Try to decompress
        print("2. 🗜️  Testing decompression...")
        if file_key.endswith('.gz'):
            try:
                decompressed = gzip.decompress(content)
                print(f"   ✅ Successfully decompressed to {len(decompressed):,} bytes")
                
                # Show first few lines
                text_content = decompressed.decode('utf-8')
                lines = text_content.split('\n')[:5]
                print(f"   📝 First 5 lines:")
                for i, line in enumerate(lines, 1):
                    print(f"      {i}. {line[:100]}...")
                    
            except Exception as e:
                print(f"   ❌ Decompression failed: {e}")
                print(f"   💡 This might not be a valid gzip file")
        else:
            print(f"   ℹ️  File doesn't end with .gz, treating as text")
            try:
                text_content = content.decode('utf-8')
                lines = text_content.split('\n')[:5]
                print(f"   📝 First 5 lines:")
                for i, line in enumerate(lines, 1):
                    print(f"      {i}. {line[:100]}...")
            except Exception as e:
                print(f"   ❌ Could not decode as text: {e}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")


def create_proper_gzip_file():
    """Create a properly formatted gzip file from local processed content"""
    if len(sys.argv) < 3:
        print("Usage: python s3cleanup.py create-gzip s3://bucket/path/file.gz")
        return
        
    s3_uri = sys.argv[2]
    
    processor = S3FileProcessor()
    
    try:
        bucket_name, file_key = processor.parse_s3_uri(s3_uri)
        
        print(f"🔧 Creating proper gzip file for: {s3_uri}")
        
        # Find the local processed file
        base_filename = os.path.basename(file_key).replace('.gz', '')
        local_pattern = f"s3_analysis_files/{base_filename}_after_*.txt"
        
        matching_files = glob.glob(local_pattern)
        
        if not matching_files:
            print(f"❌ No local processed files found matching pattern: {local_pattern}")
            print("💡 You may need to re-run the processing for this file first")
            return
        
        latest_file = max(matching_files, key=os.path.getctime)
        print(f"📁 Found local processed file: {latest_file}")
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"📊 Local file size: {len(content)} characters")
        
        # Create a properly formatted gzip file
        print("🗜️  Creating proper gzip format...")
        
        # Method 1: Use Python's gzip module with standard settings
        content_bytes = content.encode('utf-8')
        
        # Create gzip with standard compression (no custom headers)
        compressed_data = gzip.compress(content_bytes, compresslevel=6)
        
        print(f"   📊 Compressed: {len(content_bytes):,} -> {len(compressed_data):,} bytes")
        
        # Test the compression
        test_decomp = gzip.decompress(compressed_data)
        if test_decomp != content_bytes:
            raise Exception("Compression test failed")
        
        print(f"   ✅ Compression test passed")
        
        # Upload with minimal, standard metadata
        print("📤 Uploading with standard gzip format...")
        
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=compressed_data,
            ContentType='application/gzip'
            # No ContentEncoding - let it be treated as a binary gzip file
        )
        
        print(f"   ✅ Successfully uploaded: s3://{bucket_name}/{file_key}")
        
        # Final verification
        print("🔍 Final verification...")
        response = processor.s3_client.get_object(Bucket=bucket_name, Key=file_key)
        downloaded_content = response['Body'].read()
        final_decompressed = gzip.decompress(downloaded_content)
        
        print(f"   📊 Downloaded: {len(downloaded_content):,} bytes")
        print(f"   📊 Decompressed: {len(final_decompressed):,} bytes")
        print(f"   ✅ File should now be readable as a standard gzip file!")
        
    except Exception as e:
        print(f"❌ Failed to create proper gzip: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # Remove 'test' from args and test file integrity
            sys.argv.pop(1)
            test_file_integrity()
        elif sys.argv[1] == "create-gzip":
            create_proper_gzip_file()
        else:
            main()
    else:
        print("""
🚀 S3 File Processor - Usage:

PROCESS FILES:
  python s3cleanup.py s3://bucket/path/                         # Process all files (no local files)
  python s3cleanup.py s3://bucket/path/ --save-local            # Process and save local analysis files
  python s3cleanup.py s3://bucket/path/ --dry-run               # Preview without changes
  python s3cleanup.py s3://bucket/path/ --country Canada        # Keep different country
  python s3cleanup.py s3://bucket/path/ --min-lines 5           # Require minimum 5 lines to upload

ADVANCED:
  python s3cleanup.py test s3://bucket/path/file.gz             # Test if file is readable
  python s3cleanup.py create-gzip s3://bucket/path/file.gz      # Fix gzip format from local backup

FLAGS:
  --save-local     Create local analysis files (default: False)
  --dry-run        Preview mode - no changes made
  --country NAME   Country to keep (default: US)
  --min-lines N    Minimum data lines required for upload (default: 1)
        """)
        sys.exit(1)