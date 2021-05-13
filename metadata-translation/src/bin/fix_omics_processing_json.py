## add ./lib directory to sys.path so that local modules can be found
import os, sys, git_root

sys.path.append(os.path.abspath("."))
sys.path.append(os.path.abspath("./lib"))

import lib.transform_nmdc_data as tx


def main(file_path):
    ## collapse part_of
    fixed_json = tx.collapse_json_file(file_path, "part_of")
    tx.save_json(fixed_json, file_path)

    ## collapse has_input
    fixed_json = tx.collapse_json_file(file_path, "has_input")
    tx.save_json(fixed_json, file_path)

    ## collapse has_output
    fixed_json = tx.collapse_json_file(file_path, "has_output")
    tx.save_json(fixed_json, file_path)


if __name__ == "__main__":
    # main('output/nmdc_etl/test.json') # test file
    # main('output/nmdc_etl/gold_omics_processing.json') # gold omics processing
    main("output/nmdc_etl/emsl_omics_processing.json")  # emsl omics processing
