from transformers import pipeline

class EntityExtractor:
    def __init__(self, model_name="dslim/bert-base-NER"):
        self.ner_pipeline = pipeline("ner", model=model_name, grouped_entities=True)

    def extract_entities(self, text):
        ner_results = self.ner_pipeline(text)
        
        people = set()
        orgs = set()
        locations = set()
        topics = set()

        for entity in ner_results:
            label = entity['entity_group']
            word = entity['word']

            if label == "PER":
                people.add(word)
            elif label == "ORG":
                orgs.add(word)
            elif label == "LOC":
                locations.add(word)
            elif label in ["MISC"]:
                topics.add(word)

        result = {
            "people": list(people),
            "organizations": list(orgs),
            "locations": list(locations),
            "topics": list(topics)
        }

        print(f"Text: {text}")
        print(f"Extracted Entities: {result}")
        return result