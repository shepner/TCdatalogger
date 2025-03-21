import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_crime_id_order(filename):
    """Check if crime IDs are in ascending order in the response file."""
    with open(filename, 'r') as f:
        data = json.load(f)
    
    crimes = data['crimes']
    total_crimes = len(crimes)
    logger.info(f"Total number of crimes in file: {total_crimes}")
    
    prev_id = None
    out_of_order = []
    
    for crime in crimes:
        current_id = crime['id']
        if prev_id is not None and current_id < prev_id:
            out_of_order.append((prev_id, current_id))
        prev_id = current_id
    
    if out_of_order:
        logger.warning(f"Found {len(out_of_order)} instances of out-of-order IDs:")
        for prev_id, current_id in out_of_order:
            logger.warning(f"ID {current_id} comes after {prev_id}")
    else:
        logger.info("All crime IDs are in ascending order")
        
    # Print first and last IDs for reference
    if crimes:
        logger.info(f"First crime ID: {crimes[0]['id']}")
        logger.info(f"Last crime ID: {crimes[-1]['id']}")

if __name__ == '__main__':
    check_crime_id_order('response_1742470596429.json') 