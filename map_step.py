from xml.etree.ElementTree import Element

from step import Step
from crew import Crew


class MapStep(Step):
    def map_roster(self, root: Element):
        rosters = root.findall(
            "{http://service.jcms.jeppesen.com/RosterStateOutput/v5.1}roster"
        )
        crew_members = []

        for roster in rosters:
            crew = Crew()

            print(roster.attrib)

            crew.id = roster.attrib.get("crewId")
            crew.name = (
                f"{roster.attrib.get('givenName')} {roster.attrib.get('surName')}"
            )
            crew.rank = roster.attrib.get("crewCategory")
            crew.base = roster.attrib.get("homeBase")
            crew.employer = roster.attrib.get("crewCategory")

            print("Crew", crew.serialize())

            crew_members.append(crew)

        return crew_members

    def execute(self, **kwargs):
        if "roots" not in kwargs:
            raise ValueError("Missing xmls to process")

        roots = kwargs.get("roots")

        for root in roots:
            crews = self.map_roster(root)

        return crews
