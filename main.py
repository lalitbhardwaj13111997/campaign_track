import os
import logging

# from src.process_pipeline import ProcessPipeline
print(os.getcwd())
from process_pipeline import ProcessPipeline





def main_process():
    
    pipeline = ProcessPipeline()
    pipeline.process()


main_process()
