import os
from openai import OpenAI
import re

MODEL_NAME = 'gpt-4o-mini'

def make_request(prompt, file_content=None):
    api_key = os.environ.get('GPT_API_KEY', '')

    if not api_key:
        raise Exception('GPT API KEY NOT FOUND')

    client = OpenAI(api_key=api_key)

    # Create the completion
    completion = client.chat.completions.create(
        model=MODEL_NAME,
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt + '\n\n' + file_content
                    }
                ]
            },
        ]
    )

    # Return the response
    return extract_bracket_content(completion.choices[0].message.content)


def extract_bracket_content(input_string):
    match = re.search(r'\[[^\]]+\]', input_string)
    if match:
        return match.group(0)
    return None
