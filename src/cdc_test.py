import pypgoutput
import logging
import json
import time

def main():
    try:
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        host = 'localhost'
        port = 8001
        database = 'ChatUXInternal'
        user = 'postgres'
        password = 'axdLqrrbwpfYKsmQUViLlH9yQ61vdekx6A5rkoI3f9AMOG5vYmYI4jibSVp2NBeO'  # Consider using environment variables or a config file
        publication = 'mage_pub'
        slot = 'mage_slot'

        dsn = f"host={host} dbname={database} user={user} password={password} port={port}"

        cdc_reader = pypgoutput.LogicalReplicationReader(
            publication_name=publication,
            slot_name=slot,
            host=host,
            database=database,
            port=port,
            user=user,
            password=password
        )

        logger.info("Starting to read messages")
        while True:
            try:
                for message in cdc_reader:
                    logger.debug(f"Message received: {message}")
                    event_json = message.json(indent=4)
                    print(event_json)
            except StopIteration:
                time.sleep(0.5)  # Add a sleep to prevent tight loop

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()