from dataclasses import dataclass


@dataclass
class PipelineJobConfig:
    input_data_path: str
    output_data_path: str
