"""
json_producer_case.py

Stream JSON data to a Kafka topic.

Example JSON message:
{"message": "I love Python!", "author": "Eve"}

Example serialized to Kafka message:
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import pathlib  # work with file paths
import json  # work with JSON data

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


def get_custom_message() -> dict:
    """
    Fetch a custom message from the environment.
    If the message is a valid JSON string, it will be parsed.
    Otherwise, it is treated as a simple text message.
    Returns None if no custom message is provided.
    """
    custom_message = os.getenv("CUSTOM_MESSAGE")
    if custom_message:
        try:
            # Try to parse the custom message as JSON
            message_obj = json.loads(custom_message)
            logger.info("Custom message parsed as JSON.")
            return message_obj
        except json.JSONDecodeError:
            # If it is not valid JSON, wrap it in a dictionary
            logger.info("CUSTOM_MESSAGE is not valid JSON; sending as text.")
            return {"message": custom_message}
    return None

#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("buzz.json")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generators
#####################################

def generate_messages(file_path: pathlib.Path):
    """
    Read from a JSON file and yield messages one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the JSON file.

    Yields:
        dict: A dictionary containing the JSON data.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as json_file:
                logger.info(f"Reading data from file: {file_path}")

                # Load the JSON file as a list of dictionaries
                json_data: list = json.load(json_file)

                if not isinstance(json_data, list):
                    raise ValueError(
                        f"Expected a list of JSON objects, got {type(json_data)}."
                    )

                # Iterate over the entries in the JSON file
                for buzz_entry in json_data:
                    logger.debug(f"Generated JSON from file: {buzz_entry}")
                    yield buzz_entry
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {file_path}. Error: {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


def generate_custom_message(custom_msg: dict):
    """
    Continuously yield the same custom message.
    
    Args:
        custom_msg (dict): The custom message to send.
    
    Yields:
        dict: The custom message.
    """
    logger.info("Starting custom message generation.")
    while True:
        logger.debug(f"Generated custom message: {custom_msg}")
        yield custom_msg

#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated JSON messages to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    # Fetch configuration values from environment variables
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Decide which message generator to use
    custom_msg = get_custom_message()
    if custom_msg:
        logger.info("Using custom message for production.")
        message_generator = generate_custom_message(custom_msg)
    else:
        # Verify the data file exists
        if not DATA_FILE.exists():
            logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
            sys.exit(1)
        message_generator = generate_messages(DATA_FILE)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in message_generator:
            # Send message directly as a dictionary (producer handles serialization)
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
