# I24-postprocessing-lite

A LITE version of the trajectory postprocessing pipeline. 
Project website: https://i24motion.org/. 

This work is in review. The prepint is available at https://arxiv.org/abs/2212.07907.

If you use the code or techniques in this work, please consider the following citations in your publications. This allows us to aggregate statistics on the data use in publications:

> Wang, Y., Gloudemans, G., Ji, J., Teoh, Z.N., Liu, L., Zachar, G., Barbour, W., Work, D.B., 2023. Automatic vehicle trajectory data reconstruction at scale. arXiv preprint arXiv:2212.07907.

> Gloudemans, D., Wang, Y., Ji, J., Zachar, G., Barbour, W., Hall, E., Cebelak, M., Smith, L. and Work, D.B., 2023. I-24 MOTION: An instrument for freeway traffic science. Transportation Research Part C: Emerging Technologies, 155, p.104311.

The bibtex version of the references are:
```
@article{wang2023automatic,
      title={Automatic vehicle trajectory data reconstruction at scale}, 
      author={Yanbing Wang and Derek Gloudemans and Junyi Ji and Zi Nean Teoh and Lisa Liu and Gergely Zach{\'a}r and William Barbour and Daniel Work},
      year={2023},
      journal={arXiv preprint arXiv:2212.07907},
}
```

```
@article{gloudemans202324,
  title={I-24 MOTION: An instrument for freeway traffic science},
  author={Gloudemans, Derek and Wang, Yanbing and Ji, Junyi and Zachar, Gergely and Barbour, William and Hall, Eric and Cebelak, Meredith and Smith, Lee and Work, Daniel B},
  journal={Transportation Research Part C: Emerging Technologies},
  volume={155},
  pages={104311},
  year={2023},
  publisher={Elsevier}
}
```


## The architecture

The workflow of the processes are shown below. All the processes are managed by a python multiprocessing manager. The corresponding module and function name for each process is written underneath the process name. The input and output files are flat .JSON files.
![postproc_lite_architecture](https://github.com/yanb514/I24-postprocessing-lite/assets/30248823/d9890bdd-ac3c-473a-8191-942c9157d3c7)

## Benchmarking datasets
The [benchmarking datasets](https://vanderbilt.box.com/s/w0x5qxua9u8b6hi225w8xn2l5gm36upf) released along with this paper are the following:

- `SIM_GT`: TransModeler microsimulation data: 15 min, 2000 ft, 4 lanes
- `SIM_RAW`: Manually perturbed TransModeler microsimulation data
- `SIM_REC`: Reconstructed TransModeler microsimulation data from `SIM_RAW` using the proposed algorithms

The following three datasets are sourced from the I-24 MOTION validation system (I24-3D) [Gloudemans et al., 2023]. Each describes a distinct traffic scenario. The ground truth datasets comprise a total of 877,000 manually-labeled 3D bounding boxes of vehicles, derived from 57 minutes of video data collected across 16-17 cameras. Details see https://github.com/DerekGloudemans/I24-3D-dataset/tree/main. 
- `GT_i`:  Ground truth of a 60-second free-flow traffic scenario
- `RAW_i`: The raw tracking results obtained from the video recordings of GT_i.
- `REC_i`: Reconstructed data from `RAW_i` using the proposed algorithms.
- `GT_ii`:  Ground truth of a 51-second slow traffic in snowy conditions.
- `RAW_ii`: The raw tracking results obtained from the video recordings of GT_ii.
- `REC_ii`: Reconstructed data from `RAW_ii` using the proposed algorithms.
- `GT_iii`:  Ground truth of a 50-second scene of heavily congested traffic with stop-and-go waves.
- `RAW_iii`: The raw tracking results obtained from the video recordings of GT_iii.
- `REC_iii`: Reconstructed data from `RAW_iii` using the proposed algorithms.

Additional trajectory data produced from the I-24 MOTION system can be obtained from the project website
https://i24motion.org/data


## How to run

run `pp_lite()` with the following config setting.


### Configuration setting 
The config setting is located in `parameters.json`. You may specify the following parameters:

- `raw_collection`: name of the input .JSON file. E.g., "RAW_i.json". The input file should be accessible from the main directory.
- `reconciled_collection`: name of the output file. It will be saved as a JSON file in the main directory after running `pp_lite()`.
- `stitcher_args`: min cost flow related parameters, subject to tuning.
- `reconciliation_args`: trajectory rectification related parameters, subject to tuning.


## Core algorithms
There are three main algorithms in this postprocessing pipeline:
1. Fragment merging (`merge_fragments` in `merge.py`). This algorithm identifies pair-wise fragments that should be merged into one. The merging criteria is that two fragments should have time-overlap, and should be "close" in the time-space domain by some metrics. The merging operation is "associative", meaning that multiple fragments could be merged into one trajectory if any one of the fragments merges to at least another fragment in the set. Finding the merged sets is equivalent to finding connected components in an undirected graph.
2. Fragment stitching (`min_cost_flow_online_alt_path` in `min_cost_flow.py). This algorithm identifies fragments that should be stitched into one. The stitching criteria states that two fragmetns should NOT have time-overlap, and should be kinematically "close". Due to the sequential order and conflicts restriction, finding the stitched sets is equivalent to finding the min-cost-flow in a directed graph. Details of this algorithm is specified in the paper: [Online Min Cost Circulation for Multi-Object Tracking on Fragments](https://arxiv.org/abs/2311.04749).
3. Trajectory rectification (`opt2_l1_constr` in `utils_opt.py`). This step simultaneously imputes missing data, identifies and removes outliers and denoises a single trajectory independent of others. It is formulated as a convex program. Details of this algorithm is specified in our paper: [Automatic vehicle trajectory data reconstruction at scale](https://arxiv.org/abs/2212.07907).


## Output data format: 
More on data schema and layout of the testbed can be found in [[data documentation]](https://github.com/I24-MOTION/I24M_documentation).

```python
{
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["timestamp", "last_timestamp", "x_position"],
        "properties": {
            "configuration_id": {
                "bsonType": "int",
                "description": "A unique ID that identifies what configuration was run. It links to a metadata document that defines all the settings that were used system-wide to generate this trajectory fragment"
                },
            "coarse_vehicle_class": {
                "bsonType": "int",
                "description": "Vehicle class number"
                },
            
            "timestamp": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "Corrected timestamp. This timestamp may be corrected to reduce timestamp errors."
                },
            
 
            "road_segment_ids": {
                "bsonType": "array",
                "items": {
                    "bsonType": "int"
                    },
                "description": "Unique road segment ID. This differentiates the mainline from entrance ramps and exit ramps, which get distinct road segment IDs."
                },
            "x_position": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "Array of back-center x position along the road segment in feet. The  position x=0 occurs at the start of the road segment."
                },
            "y_position": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "array of back-center y position across the road segment in feet. y=0 is located at the left yellow line, i.e., the left-most edge of the left-most lane of travel in each direction."
                },
            
            "length": {
                "bsonType": "double",
                "description": "vehicle length in feet."
                },
            "width": {
                "bsonType": "double",
                "description": "vehicle width in feet"
                },
            "height": {
                "bsonType": "double",
                "description": "vehicle height in feet"
                },
            "direction": {
                "bsonType": "int",
                "description": "-1 if westbound, 1 if eastbound"
                }

            }
        }
    }
```


## Other related I-24 MOTION repositories
### I-24 MOTION data documentation
[https://github.com/I24-MOTION/I24M_documentation](https://github.com/I24-MOTION/I24M_documentation)
### I-24 MOTION improvement tracker
[https://github.com/I24-MOTION/I24M_improvement_tracker](https://github.com/I24-MOTION/I24M_improvement_tracker)
