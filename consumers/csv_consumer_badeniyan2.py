import csv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Function to check for food stall (plateau) conditions
def is_food_stall(temp):
    """Return True if the temperature is within the food stall range (150°F to 170°F)."""
    return 150 <= temp <= 190

# Function to consume the CSV and monitor the food temperature
def consume_csv(file_path):
    """Process a CSV file to detect the food stall conditions."""
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        
        # Loop through each row (time, temperature)
        for row in csv_reader:
            time = row['time']
            temperature = float(row['temperature'])
            
            # Log and check if the temperature is within the stall range
            if is_food_stall(temperature):
                logger.info(f"Time {time}: Food temperature is in stall range ({temperature}°F). Adjusting cooking settings.")
            else:
                logger.info(f"Time {time}: Food temperature is {temperature}°F, no stall detected.")

# Main entry point to run the consumer
if __name__ == "__main__":
    # Specify the path to your CSV file
    csv_file_path = 'path/to/your/food_temperatures.csv'  # Update this path

    logger.info("Starting CSV consumer...")
    consume_csv(csv_file_path)
    logger.info("CSV consumer finished processing.")
