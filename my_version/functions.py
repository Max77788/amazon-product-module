from openai import OpenAI
import os

import asyncio

client = OpenAI(api_key=os.environ.get("ANDIE_OPENAI_ACC"))
ASSISTANT_ID = os.environ.get("AMAZON_PRODUCT_ASSESSOR_ASSISTANT_ID")

def start_product_assessment(product_prompt):
    run = client.beta.threads.create_and_run(
    assistant_id=ASSISTANT_ID,
    thread={
        "messages": [
        {"role": "user", "content": product_prompt}
        ]
    }
    )
    
    return { "run_id": run.id, "thread_id": run.thread_id }

def retrieve_run_status(run_id, thread_id):
    run = client.beta.threads.runs.retrieve(
    thread_id=thread_id,
    run_id=run_id
    )
    
    run_status = run.status
    
    if run_status == "failed":
        return { "finished": True, "success":False, "message": "Run has failed", "status": run_status  }
    
    if run_status == "completed":
        return { "finished": True, "success":True, "message": "Run has completed", "status": run_status  }
    
    return { "finished": False, "success": False, "message": "Run is still running", "status": run_status  }

def retrieve_product_assessment(thread_id):
    thread_messages = client.beta.threads.messages.list(thread_id)
    
    response = thread_messages.data[0].content[0].text.value
    
    return response


# Asynchronous wrappers using asyncio's run_in_executor
async def async_start_product_assessment(product_prompt):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, start_product_assessment, product_prompt)

async def async_retrieve_run_status(run_id, thread_id):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, retrieve_run_status, run_id, thread_id)

async def async_retrieve_product_assessment(thread_id):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, retrieve_product_assessment, thread_id)