import argparse
import asyncio
import logging
import os
import sys

from automation_server_client import AutomationServer, Workqueue, Credential, WorkItemStatus
from momentum_client.manager import MomentumClientManager
from odk_tools.tracking import Tracker
from proces.config import load_excel_mapping, get_excel_mapping

momentum: MomentumClientManager
tracker: Tracker

proces_navn = "Aflys opgaver i Momentum"

async def populate_queue(workqueue: Workqueue):
    opgavetyper = get_excel_mapping()["Opgavetitel"]
    opgaver = []

    for opgavetype in opgavetyper:    
        søge_filtre = {
            "columns": [
                "title",
                "citizenId",
                "deadline",
                "type",
                "referenceId",
                "assignees",
                "state",
                "createdByName",
                "citizenDisplayName",
                "cpr",
                "targetGroup",
                "updatedByName",
                "assigneesTeamNames"
            ],
            "filters": [
                {
                "fieldName": "title",
                "values": [
                    f"{opgavetype}"
                ]
                },
                {
                "fieldName": "state",
                "values": [
                    "0"
                ]
                }
            ],
            "sort": [
                {
                "fieldName": "title",
                "ascending": True
                }
            ],
            "paging": {
                "pageNumber": 0,
                "pageSize": 100
            }
        }

        opgaver = momentum.opgaver.søg_borger_opgaver(søge_filtre)
        
        for opgave in opgaver:
            workqueue.add_item(
                data={"id": opgave["id"]},
                reference=opgave["id"],
            )


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict
 
            try:                
                momentum.opgaver.opdater_opgave_status(data["id"], momentum.opgaver.Status.aflyst)
                tracker.track_task(process_name=proces_navn)
            except Exception as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    momentum_credential = Credential.get_credential("Momentum - produktion")
    momentum = MomentumClientManager(
        base_url=momentum_credential.data["base_url"],
        client_id=momentum_credential.username,
        client_secret=momentum_credential.password,
        api_key=momentum_credential.data["api_key"],
        resource=momentum_credential.data["resource"],
    )

    tracking_credential = Credential.get_credential("Odense SQL Server")
    tracker = Tracker(
        username=tracking_credential.username, password=tracking_credential.password
    )

    project_root = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description=proces_navn)
    parser.add_argument(
        "--excel",
        help="Path to the file containing the list of citizens to process (one CPR per line)",
        default=os.path.join(project_root, "documents", "Regelsæt.xlsx"),
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Populate the queue with test data and exit",
    )
    args = parser.parse_args()

    load_excel_mapping(args.excel)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
