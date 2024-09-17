import pypgoutput
import logging
import json
import time

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    host = 'localhost'
    port = 8001
    database = 'ChatUXInternal'
    user = 'postgres'
    password = 'axdLqrrbwpfYKsmQUViLlH9yQ61vdekx6A5rkoI3f9AMOG5vYmYI4jibSVp2NBeO'  # Consider using environment variables or a config file
    publication = 'mage_pub'
    slot = 'mage_slot'

    logger.info("Starting script")

    cdc_reader = pypgoutput.LogicalReplicationReader(
        publication_name='mage_pub',
        slot_name='mage_slot',
        host='localhost',
        database='ChatUXInternal',
        port=8001,
        user='postgres',
        password='axdLqrrbwpfYKsmQUViLlH9yQ61vdekx6A5rkoI3f9AMOG5vYmYI4jibSVp2NBeO'
    )

    for message in cdc_reader:
        message_dict = message.model_dump()
        print(message_dict)

    cdc_reader.stop()

if __name__ == "__main__":
    main()