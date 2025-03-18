from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, when, regexp_replace, trim
from pyspark.sql.types import StringType
import openai
import pandas as pd
from tqdm import tqdm

def clean_email_content(df):
    """
    Clean email content by removing special characters, extra whitespace,
    and normalizing HTML entities in a PySpark DataFrame.
    """
    # Handle null values
    cleaned_df = df.withColumn(
        "content", 
        when(col("content").isNull(), "").otherwise(col("content"))
    )
    
    # Remove HTML tags
    cleaned_df = cleaned_df.withColumn(
        "content", 
        regexp_replace(col("content"), "<[^>]+>", " ")
    )
    
    # Replace common HTML entities
    cleaned_df = cleaned_df.withColumn(
        "content", 
        regexp_replace(col("content"), "&nbsp;", " ")
    )
    cleaned_df = cleaned_df.withColumn(
        "content", 
        regexp_replace(col("content"), "&amp;", "&")
    )
    cleaned_df = cleaned_df.withColumn(
        "content", 
        regexp_replace(col("content"), "&lt;", "<")
    )
    cleaned_df = cleaned_df.withColumn(
        "content", 
        regexp_replace(col("content"), "&gt;", ">")
    )
    
    # Remove excessive line breaks and tabs
    cleaned_df = cleaned_df.withColumn(
        "content", 
        regexp_replace(col("content"), "[\r\n\t]+", " ")
    )
    
    # Remove multiple spaces
    cleaned_df = cleaned_df.withColumn(
        "content", 
        regexp_replace(col("content"), "\\s+", " ")
    )
    
    # Trim leading and trailing whitespace
    cleaned_df = cleaned_df.withColumn(
        "content", 
        trim(col("content"))
    )
    
    return cleaned_df

def detect_and_translate(df, openai_api_key, batch_size=10):
    """
    Detect language of content and translate Dutch content to English using OpenAI
    
    Args:
        df: PySpark DataFrame with a 'content' column
        openai_api_key: OpenAI API key
        batch_size: Number of records to process in each batch to avoid API rate limits
        
    Returns:
        PySpark DataFrame with additional columns for language and translated content
    """
    # Configure OpenAI client
    openai.api_key = openai_api_key
    
    # Convert to Pandas for easier processing with API
    pdf = df.toPandas()
    
    # Add new columns
    pdf['detected_language'] = ''
    pdf['translated_content'] = ''
    
    # Process in batches
    for i in tqdm(range(0, len(pdf), batch_size)):
        batch = pdf.iloc[i:i+batch_size]
        
        for idx, row in batch.iterrows():
            content = row['content']
            
            # Skip empty content
            if not content or content.strip() == '':
                pdf.at[idx, 'detected_language'] = 'unknown'
                pdf.at[idx, 'translated_content'] = ''
                continue
            
            # Limit content length for API calls
            if len(content) > 1000:
                content = content[:1000] + "..."
            
            try:
                # Detect language
                language_response = openai.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a language detection assistant. Respond with only the language name."},
                        {"role": "user", "content": f"What language is this text in? Respond with only the language name (e.g., 'English', 'Dutch', etc.): {content}"}
                    ],
                    max_tokens=10
                )
                
                detected_language = language_response.choices[0].message.content.strip()
                pdf.at[idx, 'detected_language'] = detected_language
                
                # Translate if Dutch
                if detected_language.lower() == 'dutch':
                    translation_response = openai.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a Dutch to English translator. Translate the text accurately."},
                            {"role": "user", "content": f"Translate this Dutch text to English: {content}"}
                        ],
                        max_tokens=1500
                    )
                    
                    translation = translation_response.choices[0].message.content.strip()
                    pdf.at[idx, 'translated_content'] = translation
                else:
                    # For non-Dutch content, keep original
                    pdf.at[idx, 'translated_content'] = content
                    
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                pdf.at[idx, 'detected_language'] = 'error'
                pdf.at[idx, 'translated_content'] = content
    
    # Convert back to PySpark DataFrame
    spark = SparkSession.builder.getOrCreate()
    result_df = spark.createDataFrame(pdf)
    
    return result_df

# Databricks notebook cell
# Add a cell in your notebook with this code:
"""
# Install required packages
%pip install openai tqdm
"""

# Databricks notebook cell
"""
# Parameters
input_csv_path = "/dbfs/mnt/raw/hubspot/ticket_email_associations_20250318_113305.csv"
output_path = "/dbfs/mnt/raw/hubspot/translated_email_associations"
openai_api_key = "YOUR_OPENAI_API_KEY_HERE"  # Replace with your actual API key

# Read data
df = spark.read.csv(input_csv_path, header=True, inferSchema=True)
print(f"Loaded {df.count()} rows from {input_csv_path}")

# Clean the content
print("Cleaning email content...")
cleaned_df = clean_email_content(df)

# Sample to verify cleaning worked
print("\nSample of cleaned content:")
cleaned_df.select("ticket_id", "content").show(3, truncate=50)

# Detect language and translate
print("\nDetecting language and translating Dutch content...")
result_df = detect_and_translate(cleaned_df, openai_api_key, batch_size=10)

# Display results
print("\nSample results with language detection and translation:")
result_df.select("ticket_id", "detected_language", "translated_content").show(5, truncate=50)

# Save results
print(f"\nSaving results to {output_path}...")
result_df.write.csv(output_path, header=True, mode="overwrite")

print("Processing complete!")
"""

# Databricks notebook cell
"""
# Example of displaying statistics on languages detected
language_counts = result_df.groupBy("detected_language").count().orderBy("count", ascending=False)
display(language_counts)

# Count how many Dutch emails were translated
dutch_count = result_df.filter(col("detected_language").ilike("%dutch%")).count()
print(f"Total Dutch emails translated: {dutch_count}")
""" 