# Class to extract people, topics, orgs, locations
import spacy

class EntityExtractor:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")  # Use a small, efficient model

    def extract_entities(self, text):
        doc = self.nlp(text)
        people = set()
        orgs = set()
        locations = set()
        topics = set()

        for ent in doc.ents:
            if ent.label_ == "PERSON":
                people.add(ent.text)
            elif ent.label_ == "ORG":
                orgs.add(ent.text)
            elif ent.label_ in ["GPE", "LOC"]:
                locations.add(ent.text)
            elif ent.label_ in ["EVENT", "NORP", "WORK_OF_ART"]:
                topics.add(ent.text)

        return {
            "people": list(people),
            "organizations": list(orgs),
            "locations": list(locations),
            "topics": list(topics)
        }