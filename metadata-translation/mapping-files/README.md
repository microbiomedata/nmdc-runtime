## Mapping files
This directory contains files used to map vocabulary and ontology terms to database fields and/or other terminologies.  
For example, mapping GOLD paths (5-tuples) to MIxS/ENVO triads.  

The files, themselves, are stored on LBL's Google drive. To edit a particular file, click on one of the links:

* [Description_MIxS_Packages_v5.docx](https://docs.google.com/document/d/141BWGbWdTuCQ_QoqdsO_BvHW37wJuLU9xZnvnHTEtNU)
* [GOLD-Paths-to-MIxS-ENVO-triad-mapping.xlsx](https://docs.google.com/spreadsheets/d/1Tfbj0QCckk_S6ldyVJZcTdJKZHd1D9Y4whWz2CjeIcI)
* [MIxSair_20180621_GOLD_Mapping_04132020.xlsx](https://docs.google.com/spreadsheets/d/1E-XDw-_Gozxf-2eMDRC75VY8HX3UazgsCVGWfOFqi14)
* [MIxSbuiltenv_20180621_GOLD_Mapping_04172020.xlsx](https://docs.google.com/spreadsheets/d/1AYHlxRvBvKLVKLlvPcU_AvhYb_uZTzSgPTVftj47_G0)
* [MIxShostassoc_20180621_GOLD_Mapping.xlsx](https://docs.google.com/spreadsheets/d/1LJzNVDFnT8l-4Ff6h70oMQa3IThVWTaw0IoKwXwcdO0)
* [MIxShumanassoc_20180621_GOLD_Mapping_04142020.xlsx](https://docs.google.com/spreadsheets/d/1TEQIlb33ScABs6fo47UJyW8DrvYAXpJBlbDBY8c1_X8)
* [MIxShumangut_20180621_GOLD_Mapping_04152020.xlsx](https://docs.google.com/spreadsheets/d/15wmnJQbpjqq0vviElDKFGG977vt0P0xWlzlGL0o9ejA)
* [MIxShumanoral_20180621_GOLD_Mapping_04152020.xlsx](https://docs.google.com/spreadsheets/d/1lo59ZAvWnmNXmUIVMUEIscLqiAqfbIcmIL1LU3I8LPQ)
* [MIxShumanskin_20180621_GOLD_Mapping_04162020.xlsx](https://docs.google.com/spreadsheets/d/1q-zm5ChaVrvs2AKMjvZYPnYpHWgZmH8HIfY3MLe3eoo)
* [MIxShumanvaginal_20180621_GOLD_04162020.xlsx](https://docs.google.com/spreadsheets/d/1jgxYakQu_tfk0KvjflYQSJ8F-OVyOP3SRbozUdVn_9g)
* [MIxShydrocarbCores_20180621_Mapping_GOLD.xlsx](https://docs.google.com/spreadsheets/d/1RopUp6uxahqWlsG_Du-3O-DmQuG1clUEKlurZFF4ehQ)
* [MIxShydrocarbfs_20180621_v5_GOLD_Mapping.xlsx](https://docs.google.com/spreadsheets/d/1lrOjUFPkMED31euzd_t8-ADT52kC2ar6qGT2DidySpQ)
* [MIxSmatbiofilm_20180621_v5_GOLD_Mapping.xlsx](https://docs.google.com/spreadsheets/d/125xwy9tzSiks7eT43EcDqG1RVM-zKdWjpF_CZLfdyKo)
* [MIxSmisc_20180621_GOLD_Mapping.xlsx](https://docs.google.com/spreadsheets/d/1xt-ACR93IDisn2q2WYnA0EvAm7iPK_lPoaU6Sejd1ds)
* [MIxSplantassoc_20180621_v5_GOLD_Mapping.xlsx](https://docs.google.com/spreadsheets/d/13ac0mUrutBUptEnTICuxHVae2eGbQVTXGZwJwoZtHOg)
* [MIxSsediment_20180621_GOLD_Mapping_04102020.xlsx](https://docs.google.com/spreadsheets/d/1XWyrPw2BZ94PxQf03uC3aowKj3Y2WWccHuivK1aeU0E)
* [MIxSsoil_20180621_GOLD_Mapping_04102020.xlsx](https://docs.google.com/spreadsheets/d/1Fr2Bq87PCalcNVbg-M7wukTBIAPIC8e0LYQff52KD40)
* [MIxSwastesludge_20180621_GOLD_Mapping_Apr-10.xlsx](https://docs.google.com/spreadsheets/d/1kuTkBl9MHAm2agKARad9_Z3PxbEUUfeDXcLs3u_6vEI)
* [MIxSwater_20180621_GOLD_Mapping_04122020.xlsx](https://docs.google.com/spreadsheets/d/1-7I_2aDCatSru1mYskEi4i0iUEhFKPvH-vChq-E03uk)

**Note:** You must have permissions to access the LBL Google drive in order to edit the files.  
The [mixs_v4](mixs_v4.xlsx) and [mixs_v5](mixs_v5.xlsx) files are there for reference.  

Once you have finished editing the file in Google sheets, you can update the file in one of two ways:
1. Remove the file locally (e.g., `rm <file name>`) and then call `make <file name>`.
2. Delete/clean all files by calling `make clean` and then re-download all files by calling `make all`.
