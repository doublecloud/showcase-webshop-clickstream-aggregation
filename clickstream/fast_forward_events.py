import os
import pathlib

import pendulum
from rich import print as rich_print
from pydantic import BaseModel


class Event(BaseModel):
    basket_price: str
    detectedCorruption: bool
    detectedDuplicate: bool
    eventType: str
    firstInSession: bool
    item_id: str
    item_price: int
    item_url: str
    location: str
    pageViewId: str
    partyId: str
    referer: str
    remoteHost: str
    sessionId: str
    timestamp: int
    userAgentName: str


def main(target_date: str, inp: pathlib.Path, out: pathlib.Path):
    delta = 0
    start = pendulum.parse(target_date).timestamp()
    with inp.open() as events_file, out.open("w") as output:
        for i, event in enumerate(map(Event.model_validate_json, events_file)):
            if i == 0:
                delta = int(start * 1000 - event.timestamp)
            event.timestamp = int(event.timestamp + delta)
            print(event.model_dump_json(), file=output)


if __name__ == '__main__':
    target_date = "2024-07-22 10:00:00"
    inp = pathlib.Path(os.getenv('EVENTS_FILEPATH'))
    out = inp.parent / f'{inp.stem}.fastforwarded.jsonl'
    rich_print(
        "Target date for data start is",
        target_date,
        "Will shift all data from file",
        inp,
        "to start from that date and save as",
        out,
        "(I do no sanity checks: it's your responsibility that it would make sense)",
        sep="\n",
    )
    main(target_date, inp, out)
