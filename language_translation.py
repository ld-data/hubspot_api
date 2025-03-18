from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType, StructType, StructField
import pandas as pd
import openai
import os
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

def clean_email_content(df):
    """
    Clean email content by removing special characters, extra whitespace,
    and normalizing HTML entities in a PySpark DataFrame.
    """
    from pyspark.sql.functions import col, regexp_replace, trim, when
    
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
    # Set OpenAI API key
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

def process_email_content(input_csv, output_path, openai_api_key):
    """
    Main function to process email content: clean, detect language, and translate
    
    Args:
        input_csv: Path to the input CSV file
        output_path: Path to save the output
        openai_api_key: OpenAI API key
    """
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Email Content Processing") \
        .getOrCreate()
        
    # Read CSV file
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    
    # Clean content
    print("Cleaning email content...")
    cleaned_df = clean_email_content(df)
    
    # Detect language and translate
    print("Detecting language and translating content...")
    result_df = detect_and_translate(cleaned_df, openai_api_key)
    
    # Save results
    print(f"Saving results to {output_path}...")
    result_df.write.csv(output_path, header=True, mode="overwrite")
    
    print("Processing complete!")
    return result_df

# Example usage
if __name__ == "__main__":
    # Get OpenAI API key from environment variable or input
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        openai_api_key = input("Enter your OpenAI API key: ")
    
    # Process the data
    input_file = "ticket_email_associations_20250318_113305.csv"
    output_path = "translated_email_associations"
    
    result_df = process_email_content(input_file, output_path, openai_api_key)
    
    # Show sample results
    print("\nSample results:")
    result_df.select("ticket_id", "detected_language", "translated_content").show(5, truncate=50) 