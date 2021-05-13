# -- ETL commands --
.PHONY: run-etl build-test-datasets build-example-db build-merged-db

# directories for output and data
etl_build_dir := src/bin/output
etl_data_dir := src/data
etl_example_dir := examples

# files produced by etl
etl_db := $(etl_build_dir)/nmdc_database.json
etl_db_zip := $(etl_build_dir)/nmdc_database.json.zip
etl_example_db := $(etl_build_dir)/nmdc_example_database.json
etl_test_sets := study_test.json gold_project_test.json biosample_test.json readQC_data_objects_test.json readQC_activities_test.json mg_assembly_data_objects_test.json mg_assembly_activities_test.json emsl_data_object_test.json emsl_project_test.json

# add directories to test set files
etl_test_set_files := $(foreach set, $(etl_test_sets), $(etl_build_dir)/$(set))

test-etl-vars:
	@echo $(etl_db)
	@echo $(etl_db_zip)
	@echo $(etl_example_db)
	@echo $(etl_test_sets)
	@echo $(etl_test_set_files)

run-etl:
# runs the ETL script, creates the nmdc datbase and test/example files
# create needed dirs
	mkdir -p src/bin/output/nmdc_etl

# navigate to directory and execute pipeline script
	cd src/bin/ && python execute_etl_pipeline.py

# zip output and move to data directory
	rm -f $(etl_db_zip) # remove old copy of zipped db
	zip $(etl_db_zip) $(etl_db) # zip new copy
	cp $(etl_db_zip) $(etl_data_dir) # cp new db to data directory

# copy example database to examples directory
	cp $(etl_example_db) $(etl_example_dir)

# copy test datasets to examples
	cp $(etl_test_set_files) $(etl_example_dir)

build-test-datasets:
# runs the ETL scipt, but ONLY creates the test dataset
# create needed dirs
	mkdir -p src/bin/output/nmdc_etl

# navigate to directory and execute pipeline script
	cd src/bin/ && python execute_etl_pipeline.py --testdata --no-etl --no-exdb --no-mergedb

# copy test datasets to examples
	cp $(etl_test_set_files) $(etl_example_dir)

build-example-db:
# runs the ETL scipt, but ONLY creates the example database
# create needed dirs
	mkdir -p src/bin/output/nmdc_etl

# navigate to directory and execute pipeline script
	cd src/bin/ && python execute_etl_pipeline.py --exdb --no-testdata --no-etl --no-mergedb

# copy example database to examples directory
	cp $(etl_example_db) $(etl_example_dir)

build-merged-db:
# runs the ETL scipt, but ONLY creates the merged data source used as input for the ETL pipeline
# create needed dirs
	mkdir -p src/bin/output/nmdc_etl

# navigate to directory and execute pipeline script
	cd src/bin/ && python execute_etl_pipeline.py --only-mergedb
