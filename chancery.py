from bs4 import BeautifulSoup
from nlp import string_to_entities


def clean_text(input_string):
    """
    Use Beautiful Soup to turn HTML into plaintext

    :param input_string: string (which might be HTML)
    :return: string (plaintext)
    """
    soup = BeautifulSoup(input_string, features="html.parser")
    text = soup.get_text()
    return text


def text_between(input_string, start_phrase):
    """

    :param input_string:
    :param start_phrase:
    :return:
    """
    if start_phrase in input_string:
        left_truncated = input_string[input_string.index(start_phrase) + len(start_phrase):]
        try:
            period_index = left_truncated.index(".")
            return left_truncated[:period_index].strip()
        except ValueError:
            return left_truncated.strip()
    else:
        return


def parse_description(description, spacy_nlp):
    """

    :param description:
    :return:
    """
    text = clean_text(description)
    short_title = {
        "text": text_between(text, "Short title:"),
        # "entities": string_to_entities(
        #     input_string=text_between(text, "Short title:"),
        #     nlp=spacy_nlp,
        #     ent_types=["PERSON", "ORG"],
        # ).get("entities_by_type"),
    }
    plaintiffs = {"text": text_between(text, "Plaintiffs:"),
                  "entities": string_to_entities(
                      input_string=text_between(text, "Plaintiffs:"),
                      nlp=spacy_nlp,
                      ent_types=["PERSON", "ORG"],
                  ).get("entities_by_type"),
                  }
    defendants = {"text": text_between(text, "Defendants:"),
                  "entities": string_to_entities(
                      input_string=text_between(text, "Defendants:"),
                      nlp=spacy_nlp,
                      ent_types=["PERSON", "ORG"],
                  ).get("entities_by_type"),
                  }
    subject = {"text": text_between(text, "Subject:"),
               # "entities": string_to_entities(
               #     input_string=text_between(text, "Subject:").strip(),
               #     nlp=spacy_nlp,
               #     ent_types=["PERSON", "ORG", "GPE", "FAC", "LOC", "DATE"],
               # ).get("entities_by_type"),
               }
    document_type = {"text": text_between(text, "Document type:")}
    data = dict(
        # _input=description,
        short_title=short_title,
        plaintiffs=plaintiffs,
        defendants=defendants,
        subject=subject,
        plaintext=text,
        document_type=document_type
    )
    return data


if __name__ == "__main__":
    import json
    import spacy

    nlp = spacy.load("en_core_web_sm")
    foo = "<scopecontent><p>Short title: Newton v Smith. </p><p>Plaintiffs: Isaac Newton. </p><p>Defendants: Benjamin Smith. </p><p>Subject: personal estate of Hannah Smith, widow, Woolsthorpe, Lincolnshire. </p><p>Document type: bill only</p></scopecontent>"
    bar = "Short title: Knight v Thomas. Plaintiffs: Elizabeth Knight, widow. Defendants: Samuel Thomas, Dame Mary Thomas, John Bumpstead, William Erbury, Samuel Erbury, John Man, Matthew Banks and others. Subject: property in the parish of St Olave, Southwark, Surr"
    # print(json.dumps(parse_description(foo, spacy_nlp=nlp), indent=2, sort_keys=True))
    print(json.dumps(parse_description(bar, spacy_nlp=nlp), indent=2, sort_keys=True))
