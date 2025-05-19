"""Plan(X)ton Class: Simple scripts specific to data injestion and the user services role in CTDS-PlanX
Author: Dan Biber
Date: 07/16/24
rev: 0.00.01
Description: A class that evolved as an efficient form of ingestion of data into Gen3 technology for the PlanX team.
"""

import pandas as pd
import re
import os
import json
import sys
import gen3
import io
import tempfile
import requests
import glob
import numpy as np
from datetime import datetime
from io import StringIO
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
from gen3.tools.indexing import validate_manifest_format
from gen3.tools.indexing.index_manifest import index_object_manifest


class planxton_midrc:
    """Scripts that aid in ingestion of data into multiple MIDRC data commons instances

    Args:
      auth (Gen3Auth): A Gen3Auth class instance
      sub (Gen3Submisson) = A Gen3Submission class instance
    Examples:
      This generates the planxton class pointed at the MIDRC staging and validatestaging commons while using
      the credentials and apis for both staging and

      >>> endpoint = 'https://preprod.gen3.biodatacatalyst.nhlbi.nih.gov'
      ... auth = Gen3Auth(endpoint, refresh_file = 'creds.json')
      ... sub = Gen3Submission(endpoint, auth)
      ... plx = planxton(endpoint, auth, sub)
    """

    ########################################################################
    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint
        self.auth = Gen3Auth(endpoint, refresh_file=auth_provider)
        self.sub = Gen3Submission(endpoint, self.auth)

    ########################################################################
    ## Initilizes planxton without having to import Gen3Auth and Gen3Submission to main.py
    ########################################################################

    def init_planxton(endpoint, refresh_file):
        auth = Gen3Auth(endpoint, refresh_file=refresh_file)
        submission = Gen3Submission(endpoint, auth)
        return planxton(endpoint, auth, submission)

    ########################################################################
    ########################################################################
    ##
    ##General Functions
    ##
    ########################################################################
    ########################################################################
    def expansion(self):
        import importlib.util
        import os

        url = "https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py"
        api_url = "https://api.github.com/repos/cgmeyer/gen3sdk-python/commits?path=expansion/expansion.py"
        # Download expansion.py
        response = requests.get(url)
        if response.status_code == 200:
            filename = "expansion.py"
            with open(filename, "w") as file:
                file.write(response.text)
            # Dynamically load the Gen3Expansion class
            spec = importlib.util.spec_from_file_location("expansion", filename)
            expansion_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(expansion_module)
            # Create an instance of Gen3Expansion
            self.exp = expansion_module.Gen3Expansion(
                self._endpoint, self._auth_provider, self.sub
            )
            # Fetch the last updated date from GitHub API
            api_response = requests.get(api_url)
            if api_response.status_code == 200:
                commit_data = api_response.json()
                if commit_data:  # Check if commit data is not empty
                    last_updated = commit_data[0]["commit"]["committer"]["date"]
                    print(
                        f"Expansion loaded successfully. Last Updated: {last_updated}"
                    )
                else:
                    print(
                        "Expansion loaded successfully. Last Updated date could not be determined."
                    )
            else:
                print(
                    "Expansion loaded successfully, but failed to fetch last updated date from GitHub."
                )
            # Delete file after loading
            os.remove(filename)
            # Return the exp object
            return self.exp
        else:
            print("Failed to download expansion.py")
            return None

    ########################################################################
    ##Helper Functions
    ########################################################################

    def fetch_programs(self):
        txt = "You have access to the following Programs: "
        programs = json.dumps(self.sub.get_programs(), indent=4, sort_keys=True)

        return txt + programs

    def fetch_projects(self):
        txt = "You have access to the following Projects: "
        projects = json.dumps(self.exp.get_project_ids(), indent=4, sort_keys=True)

        return txt + projects

    def create_ppb(self, program, project, org, date, txt=False):
        ppb = [program, project, org, date]
        ppb_txt = program + "-" + project + "_" + org + "_" + date
        return ppb_txt if txt else ppb

    def extract_pid(self, ppb):
        pid = ppb[0] + "-" + ppb[1]
        return pid

    def clean_tsv_file(self, tsvs, remove_tsvs):
        for tname in list(tsvs):
            if tname.startswith("MISSING") or tname in remove_tsvs:
                del tsvs[tname]
        return tsvs

    def get_tsv_file(self, ppb, path, clean=False, remove_tsvs=None):
        os.chdir(path)
        print(f"Getting .tsv files from: {path}\n")
        org = ppb[2]
        date = ppb[3]

        tfiles = glob.glob("*_{}.tsv".format(date))
        tsv_regex = re.compile(r"(.*)_{}_{}\.tsv$".format(org, date))
        tnames = [tsv_regex.match(i).groups()[0] for i in tfiles if tsv_regex.match(i)]
        tsvs = dict(zip(tnames, tfiles))

        if clean:
            tsvs = self.clean_tsv_file(tsvs, remove_tsvs)

        return tsvs

    def series_get_tsv_file(self, ppb, tsvs, path):
        print(f"Getting series .tsv files from: {path}\n")
        org = ppb[2]
        date = ppb[3]
        batch_name = org + "_" + date

        series_regex = re.compile(r"(.*_series)(_{}.*)".format(batch_name))
        series_tsvs = {k: v for k, v in tsvs.items() if series_regex.match(v)}

        return series_tsvs

    def get_dd_node_order(
        self,
        root_node="project",
        excluded_schemas=[
            "_definitions",
            "_settings",
            "_terms",
            "program",
            "project",
            "root",
            "data_release",
            "metaschema",
        ],
    ):
        """Utilize the Expansion 'get_submission_order' function to return an ordered list of nodes
        Args:
          root_node: The 'highest' node relevant to the submission order
          excluded_schemas: nodes collected from the data dictionary that are not relevant
        Returns:
          submission_order:  A list of tuples that contain the node_type and the order in which to submit (dict)
        """
        submission_order = dict(
            self.exp.get_submission_order(root_node, excluded_schemas)
        )
        return submission_order

    def check_submission_order(self, node_dict):
        """Checks if the node_list is in the correct order compared to the data dictionary

        Args:
          node_list: A list of node names to submitted (list)

        Returns:
          bool: True if node_list is ordered correctly
          ooo_node: The first node in the list that is out of order
        """
        submission_order = self.get_dd_node_order()

        # Extract the keys (node types) from node_dict in the order they appear
        node_list = list(node_dict.keys())

        # Iterate through the node_list and compare the submission orders
        for i in range(1, len(node_list)):
            prev_node = node_list[i - 1]
            curr_node = node_list[i]

            if submission_order[curr_node] < submission_order[prev_node]:
                return (
                    False,
                    f"Node list is out of order. The first out-of-order node is '{curr_node}'.",
                )

        return True, "Node list is in the correct order."

    ########################################################################
    ## Pre-Ingest QC
    ########################################################################
    def sort_batch_tsvs(self, ppb, batch_dir):
        """Sorts the TSVs provided by a MIDRC data submitter into manifests and node submission TSVs.

        Args:
            batch(str): the name of the batch, e.g., "RSNA_20230303"
            batch_dir(str): the full path of the local directory where the batch TSVs are located.

        Returns:
          batch_tsvs(dict): Dictionary of the tsvs in the batch

        """
        # Process the ppb object
        org = ppb[2]
        date = ppb[3]
        batch_name = org + "_" + date

        print(f"Processing {batch_name} batch metadata TSVs.")

        tsvs = []
        for file in os.listdir(batch_dir):
            if file.endswith(".tsv"):
                tsvs.append(os.path.join(batch_dir, file))

        nodes = self.exp.get_submission_order()
        nodes = [i[0] for i in nodes]

        node_tsvs = {}
        clinical_manifests, image_manifests = [], []
        other_tsvs, nomatch_tsvs = [], []
        node_regex = r".*/(\w+)_{}\.tsv".format(batch_name)

        for tsv in tsvs:
            # print(tsv)
            if "manifest" in tsv:
                if "clinical" in tsv:
                    clinical_manifests.append(tsv)
                elif "image" in tsv or "imaging" in tsv:
                    image_manifests.append(tsv)
            else:
                match = re.findall(node_regex, tsv, re.M)
                # print(match)

                if not match:
                    nomatch_tsvs.append(tsv)
                else:
                    node = match[0]
                    if node in nodes:
                        node_tsvs[node] = tsv
                    elif node + "_file" in nodes:
                        node_tsvs["{}_file".format(node)] = tsv
                    else:
                        other_tsvs.append({node: tsv})
        batch_tsvs = {
            "batch": batch_name,
            "node_tsvs": node_tsvs,
            "image_manifests": image_manifests,
            "clinical_manifests": clinical_manifests,
            "other_tsvs": other_tsvs,
            "nomatch_tsvs": nomatch_tsvs,
        }
        return batch_tsvs

    def check_case_ids(self, df, node, cids):
        """
        Check that all case IDs referenced across dataset are in case TSV; "cids" = "case_ids"

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            cids(list): the list of case IDs provided in the batch case TSV
        """
        errors = []
        extra_cids = []
        if node != "case":
            if "case_ids" in df:
                df_cids = list(set(df["case_ids"]))
            elif "cases.submitter_id" in df:
                df_cids = list(set(df["cases.submitter_id"]))
            else:
                error = "Didn't find any case IDs in the {} TSV!".format(node)
                print(error)
                errors.append(error)
                df_cids = []
            # print("Found {} case IDs in the {} TSV.".format(len(cids),node_id))
            extra_cids = list(set(df_cids).difference(cids))

            if len(extra_cids) > 0:
                error = "{} TSV contains {} case IDs that are not present in the case TSV!\n\t{}\n\n".format(
                    node, len(extra_cids), extra_cids
                )
                print(error)
                errors.append(error)

        return errors

    def check_type_field(self, df, node):
        """
        Check that the type of all values for properties in a node submission TSV match the data dictionary type

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        if not "type" in df:
            error = "{} TSV does not have 'type' header!".format(node)
            print(error)
            errors.append(error)
        else:
            if not list(set(df.type))[0] == node:
                error = "{} TSV does not have correct 'type' field.".format(node)
                print(error)
                errors.append(error)
        return errors

    def check_submitter_id(self, df, node):
        """
        Check that the submitter_id column is complete and doesn't contain duplicates.
        "sids" is short for "submitter_ids".

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        if not "submitter_id" in df:
            error = "{} TSV does not have 'submitter_id' header!".format(node)
            print(error)
            errors.append(error)
        else:
            sids = list(set(df.submitter_id))
            if not len(sids) == len(df):
                error = "{} TSV does not have unique submitter_ids! Submitter_ids: {}, TSV Length: {}".format(
                    node, len(sids), len(df)
                )
                print(error)
                errors.append(error)
        return errors

    def check_links(self, df, node, dd):
        """
        Check whether link headers are provided in a node submission TSV
        In many cases, node TSVs simply link to the case node, and submitters just provide the "case_ids" column, but we'll check anyways.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            dd(dictionary): the data dictionary being used, get with Gen3Submission.get_dictionary_all()
        """
        errors = []
        links = self.exp.list_links(node, dd)
        if "core_metadata_collections" in links:
            links.remove("core_metadata_collections")
        if "core_metadata_collections.submitter_id" in links:
            links.remove("core_metadata_collections.submitter_id")
        for link in links:
            link_col = "{}.submitter_id".format(link)
            if link_col not in df:
                error = "'{}' link header not found in '{}' TSV.".format(link_col, node)
                print(
                    error
                )  # this is not necessarily an error, as some links may be optional, but must have at least 1 link
                errors.append(error)
        return errors

    # 4) special characters
    def check_special_chars(
        self, node, batch_tsvs
    ):  # probably need to add more types of special chars to this
        """
        Check for special characters that aren't compatible with Gen3's sheepdog submission service.

        Args:
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        filename = batch_tsvs["node_tsvs"][node]
        with open(filename, "rb") as tsv_file:
            lns = tsv_file.readlines()
            count = 0
            for ln in lns:
                count += 1
                if b"\xe2" in ln:
                    error = "{} TSV has special char in line {}: {}".format(
                        node, count, ln
                    )
                    print(error)
                    errors.append(error)
        return errors

    def check_required_props(self, df, node, dd, exclude_props):
        """
        Check whether all required properties for a node are provided in the submission TSV.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            dd(dictionary): the data dictionary being used, get with Gen3Submission.get_dictionary_all()
        """
        errors = []
        links = self.exp.list_links(node, dd)
        any_na = df.columns[df.isna().any()].tolist()
        required_props = list(
            set(dd[node]["required"]).difference(links).difference(exclude_props)
        )
        for prop in required_props:
            if prop not in df:
                error = "{} TSV does not have required property header '{}'!".format(
                    node, prop
                )
                print(error)
                errors.append(error)
            elif prop in any_na:
                error = "{} TSV does not have complete data for required property '{}'!".format(
                    node, prop
                )
                print(error)
                errors.append(error)
        return errors

    def check_completeness(self, df, node):
        """
        Report on whether any properties in column headers have all NA/null values.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        all_na = df.columns[df.isna().all()].tolist()
        if len(all_na) > 0:
            error = "'{}' TSV has all NA values for these properties: {}".format(
                node, all_na
            )
            print(error)
            errors.append(error)
        return errors

    # 7) prop types
    def check_prop_types(self, df, node, dd, exclude_props):
        """
        Check that the types of properties match their values.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            dd(dictionary): the data dictionary being used, get with Gen3Submission.get_dictionary_all()
        """
        errors = []
        all_na = df.columns[df.isna().all()].tolist()
        links = self.exp.list_links(node, dd)
        required_props = list(
            set(dd[node]["required"]).difference(links).difference(exclude_props)
        )
        if all_na == None:
            props = list(
                set(dd[node]["properties"])
                .difference(links)
                .difference(required_props)
                .difference(dd[node]["systemProperties"])
                .difference(exclude_props)
            )
        else:
            props = list(
                set(dd[node]["properties"])
                .difference(links)
                .difference(required_props)
                .difference(dd[node]["systemProperties"])
                .difference(exclude_props)
                .difference(all_na)
            )
        for prop in props:
            if prop in df:
                if "type" in dd[node]["properties"][prop]:
                    etype = dd[node]["properties"][prop]["type"]  # expected type
                    if etype == "array":
                        if "items" in dd[node]["properties"][prop]:
                            etype = dd[node]["properties"][prop]["items"]
                            if "type" in dd[node]["properties"][prop]["items"]:
                                etype = dd[node]["properties"][prop]["items"]["type"]

                    d = df[prop].dropna()
                    if etype == "integer":
                        try:
                            d = d.astype(int)
                        except Exception as e:
                            error = "'{}' prop should be integer, but has non-integer values: {}".format(
                                prop, e
                            )
                            print(error)
                            errors.append(error)
                    elif etype == "number":
                        try:
                            d = d.astype(float)
                        except Exception as e:
                            error = "'{}' prop should be integer, but has non-integer values: {}".format(
                                prop, e
                            )
                            print(error)
                            errors.append(error)
                            continue  # Skip to the next property if conversion fails
                    if "minimum" in dd[node]["properties"][prop]:
                        minimum = dd[node]["properties"][prop]["minimum"]
                        if (d < minimum).any():
                            error = (
                                "'{}' property has values below the minimum: {}".format(
                                    prop, d[d < minimum]
                                )
                            )
                            print(error)
                            errors.append(error)
                    if "maximum" in dd[node]["properties"][prop]:
                        maximum = dd[node]["properties"][prop]["maximum"]
                        if (d > maximum).any():
                            error = (
                                "'{}' property has values above the maximum: {}".format(
                                    prop, d[d > maximum]
                                )
                            )
                            print(error)
                            errors.append(error)
                    elif etype == "boolean":
                        vals = list(set(d))
                        wrong_vals = list(
                            set(vals).difference(
                                ["True", "False", "true", "false", "TRUE", "FALSE"]
                            )
                        )
                        if len(wrong_vals) > 0:
                            error = (
                                "'{}' property has incorrect boolean values: {}".format(
                                    prop, wrong_vals
                                )
                            )
                            print(error)
                            errors.append(error)
                    else:
                        d = d.convert_dtypes(
                            infer_objects=True,
                            convert_string=True,
                            convert_integer=True,
                            convert_boolean=True,
                            convert_floating=True,
                        )
                        # itype = d.dtypes[prop] # inferred type
                        itype = d.dtype  # inferred type
                        # if itype == 'Int64':
                        #     itype = 'integer'
                        if not etype == itype:
                            error = "'{}' property has inferred type '{}' and not the expected type: '{}'".format(
                                prop, itype, etype
                            )
                            print(error)
                            errors.append(error)

                    # to do: Check for min/max of number/int properties
                    if "minimum" in dd[node]["properties"][prop]:  #
                        min = dd[node]["properties"][prop]["minimum"]
                        # for each value of d, are any less than min or greater than max

                elif "enum" in dd[node]["properties"][prop]:
                    enums = dd[node]["properties"][prop]["enum"]
                    vals = list(set(df[prop].dropna()))
                    wrong_vals = list(set(vals).difference(enums))
                    if len(wrong_vals) > 0:
                        error = "'{}' property has incorrect enum values: {}".format(
                            prop, wrong_vals
                        )
                        print(error)
                        errors.append(error)

            else:
                error = "'{}' property in dictionary is not in the '{}' TSV.".format(
                    prop, node
                )
                print(error)
                errors.append(error)

        # check that columns in TSV are correctly named and present in data dictionary for that node
        df_props = list(df)
        extra_props = list(set(df_props).difference(list(set(dd[node]["properties"]))))
        for link in links:
            if link in extra_props:
                extra_props.remove(link)
            alt_link = link + ".submitter_id"
            if alt_link in extra_props:
                extra_props.remove(alt_link)
        if len(extra_props) > 0:
            error = "'{}' properties in the {} TSV not in the data dictionary.".format(
                extra_props, node
            )
            print(error)
            errors.append(error)
        errors = list(set(errors))
        return errors

    def get_case_node(self, ppb):
        """
        get get the case node as dataframe for a given project with expansion

        Arg:
          ppb(list): program, project, batch object used in planxton_midrc
          batch_tsvs(dict): The output from sort_batch_tsvs

        Return:
          A dataframe of the case node for the given project
        """
        program_id = ppb[0]
        project_id = ppb[1]

        print("Downloading case node")
        json_case = self.sub.export_node(program_id, project_id, "case", "json")
        print("Downloaded case node")
        df = pd.DataFrame(json_case["data"])
        return df

    def case_xcheck(self, submitted_case_node, existing_case_node):
        """
          Compare all the case_ids and some metadata from the new batch against the existing case_ids and metadata.

        Args:
          submittted_case_node(df): The case node submitted with the current batch as a dataframe.
          existing_case_node(df): The dataframe from "get_case_node".

        Returns:
          dict: A dictionary containing all duplicate case_ids in which metadata (race, age_at_index, sex, ethnicity) does not match.
        """
        required_columns = ["submitter_id", "race", "age_at_index", "sex", "ethnicity"]

        for col in required_columns:
            if col not in submitted_case_node.columns:
                raise ValueError(
                    f"Column '{col}' is missing from the submitted case node dataframe."
                )
            if col not in existing_case_node.columns:
                raise ValueError(
                    f"Column '{col}' is missing from the existing case node dataframe."
                )

        # Merge the dataframes on case_id to align the cases
        merged_df = submitted_case_node.merge(
            existing_case_node, on="submitter_id", suffixes=("_submitted", "_existing")
        )

        print(f"There are {merged_df.shape[0]} matching case ids.")

        # Dictionary to store mismatched cases
        mismatched_cases = {}

        # Check for mismatches and add to the dictionary
        for _, row in merged_df.iterrows():
            mismatches = {}
            if row["race_submitted"] != row["race_existing"]:
                mismatches["race"] = {
                    "submitted": row["race_submitted"],
                    "existing": row["race_existing"],
                }
            if row["age_at_index_submitted"] != row["age_at_index_existing"]:
                mismatches["age_at_index_gt89"] = {
                    "submitted": row["age_at_index_submitted"],
                    "existing": row["age_at_index_existing"],
                }
            if row["sex_submitted"] != row["sex_existing"]:
                mismatches["sex"] = {
                    "submitted": row["sex_submitted"],
                    "existing": row["sex_existing"],
                }
            if row["ethnicity_submitted"] != row["ethnicity_existing"]:
                mismatches["ethnicity"] = {
                    "submitted": row["ethnicity_submitted"],
                    "existing": row["ethnicity_existing"],
                }

            if mismatches:
                mismatched_cases[row["submitter_id"]] = mismatches

        return mismatched_cases

    def get_img_study_node(self, ppb):
        """
        get the image_study node as dataframe for a given project with expansion

        Arg:
          ppb(list): program, project, batch object used in planxton_midrc
          batch_tsvs(dict): The output from sort_batch_tsvs

        Return:
          A dataframe of the case node for the given project
        """
        program_id = ppb[0]
        project_id = ppb[1]

        print("Downloading image_study node")
        json_img_study = self.sub.export_node(
            program_id, project_id, "imaging_study", "json"
        )
        print("Downloaded image_study node")
        df = pd.DataFrame(json_img_study["data"])
        return df

    def img_study_xcheck(self, submitted_img_study, existing_img_study):
        """
        Compare all the imaging_study submitter_ids and some metadata from the new batch against the existing submitter_ids and metadata.

        Args:
          submitted_img_study(df): The case node submitted with the current batch as a dataframe.
          existing_img_study(df): The dataframe from "get_case_node".

        Returns:
          dict: A dictionary containing all duplicate case_ids in which metadata (age_at_imaging, age_at_imaging_gt89, body_part_examined, study_modality)
          does not match.
        """
        required_columns = [
            "submitter_id",
            "age_at_imaging",
            "age_at_imaging_gt89",
            "body_part_examined",
            "study_modality",
        ]

        for col in required_columns:
            if col not in submitted_img_study.columns:
                raise ValueError(
                    f"Column '{col}' is missing from the submitted case node dataframe."
                )
            if col not in existing_img_study.columns:
                raise ValueError(
                    f"Column '{col}' is missing from the existing case node dataframe."
                )

        # Coerce all of the relevant columns into strings for comparison
        for col in required_columns:
            submitted_img_study[col] = submitted_img_study[col].astype(str)
            existing_img_study[col] = existing_img_study[col].astype(str)

        # Merge the dataframes on case_id to align the cases
        merged_df = submitted_img_study.merge(
            existing_img_study, on="submitter_id", suffixes=("_submitted", "_existing")
        )

        print(f"There are {merged_df.shape[0]} matching imaging_study ids.")

        # Dictionary to store mismatched cases
        mismatched_cases = {}

        # Check for mismatches and add to the dictionary
        for _, row in merged_df.iterrows():
            mismatches = {}
            if row["age_at_imaging_submitted"] != row["age_at_imaging_existing"]:
                mismatches["age_at_imaging"] = {
                    "submitted": row["age_at_imaging_submitted"],
                    "existing": row["age_at_imaging_existing"],
                }
            if (
                row["age_at_imaging_gt89_submitted"]
                != row["age_at_imaging_gt89_existing"]
            ):
                mismatches["age_at_imaging_gt89"] = {
                    "submitted": row["age_at_imaging_gt89_submitted"],
                    "existing": row["age_at_imaging_gt89_existing"],
                }
            if (
                row["body_part_examined_submitted"]
                != row["body_part_examined_existing"]
            ):
                mismatches["body_part_examined"] = {
                    "submitted": row["body_part_examined_submitted"],
                    "existing": row["body_part_examined_existing"],
                }
            if row["study_modality_submitted"] != row["study_modality_existing"]:
                mismatches["study_modality"] = {
                    "submitted": row["study_modality_submitted"],
                    "existing": row["study_modality_existing"],
                }

            if mismatches:
                mismatched_cases[row["submitter_id"]] = mismatches

        return mismatched_cases

    def get_series_nodes(self, ppb):
        """
        get all series nodes as dataframes for a given project with expansion

        Arg:
          ppb(list): program, project, batch object used in planxton_midrc
          batch_tsvs(dict): The output from sort_batch_tsvs

        Return:
          A dictionary of dataframes with series_nodes as keys and dfs as values for the given project
        """
        program_id = ppb[0]
        project_id = ppb[1]

        dd_order = self.get_dd_node_order()

        series_nodes_names = []

        for k in dd_order:
            if "series" in k:
                series_nodes_names.append(str(k))

        series_df_dict = {}

        for node in series_nodes_names:
            json_node = self.sub.export_node(program_id, project_id, node, "json")
            df = pd.DataFrame(json_node["data"])
            print(f"\rDownloaded {node} has a shape of {df.shape}", end="")
            series_df_dict[node] = df
        return series_df_dict

    def series_uid_xcheck(self, batch_tsvs, series_df_dict):
        """
        Compare all the series_uids from the new batch against all existing series_uids.

        Args:
          batch_tsvs(dict): The batch_tsv dictionary created by "sort_batch_tsvs".
          series_df_dict(dict of dfs): The series_df_dict from "get_series_nodes".

        Returns:
          dict: A dictionary containing all duplicate series_uids and the node from which they originate.
        """
        existing_series_uids = {}

        # Collect existing submitter_ids from series_df_dict
        for existing_node, df in series_df_dict.items():
            if not df.empty:
                existing_series_uids[existing_node] = df["submitter_id"]

        conflicting_submitter_ids = {}

        # Check each node in batch_tsvs['node_tsvs'] for conflicting submitter_ids
        for node, file_path in batch_tsvs["node_tsvs"].items():
            if "series" in node:
                print(f"Processing: {node}")
                df = pd.read_csv(file_path, sep="\t")

                # Check for overlaps in submitter_id for each existing_node
                for existing_node, existing_ids in existing_series_uids.items():
                    # print(len(existing_ids))
                    try:
                        overlapping_submitter_ids = df["submitter_id"][
                            df["submitter_id"].isin(existing_ids)
                        ]
                    except KeyError:
                        print(
                            f"'submitter_id' was not a column found in {node}. Matching submitted 'series_uid' to existing {existing_node} 'submitter_id' was attempted instead."
                        )
                        try:
                            overlapping_submitter_ids = df["series_uid"][
                                df["series_uid"].isin(existing_ids)
                            ]
                        except KeyError:
                            print(
                                f"'series_uid' was also not found in {node}. Skipping this node."
                            )
                            continue

                    # Only add conflicts if there are overlapping submitter IDs or series_uids
                    if not overlapping_submitter_ids.empty:
                        if node not in conflicting_submitter_ids:
                            conflicting_submitter_ids[node] = {}
                        conflicting_submitter_ids[node][
                            existing_node
                        ] = overlapping_submitter_ids.tolist()

        return conflicting_submitter_ids

    def pre_ingest_qc_check(
        self,
        ppb,
        batch_tsvs,
        report_output_dir,
        exclude_props=[  # submitters don't provide these properties, so remove them from QC check
            # case props not provided by submitters
            "datasets.submitter_id",
            "token_record_id",
            "linked_external_data",
            # series_file props not provided by submitters
            "file_name",
            "md5sum",
            "file_size",
            "object_id",
            "storage_urls",
            "core_metadata_collections.submitter_id",
            "core_metadata_collections",
            "associated_ids",
            # imaging_study props not provided by submitters
            "loinc_code",
            "loinc_system",
            "loinc_contrast",
            "loinc_long_common_name",
            "loinc_method",
            "days_from_study_to_neg_covid_test",
            "days_from_study_to_pos_covid_test",
        ],
    ):
        """
        This is a wrapper function to run all of the previous functions to check batch_tsvs

        Arg:
          batch_tsvs(dict): The batch_tsv dictionary created by "sort_batch_tsvs"
          exclude_props(list): This is the default place the excluded props will be listed as default.
          report_output_dir(path): A directory that a report text file is output

        Return:
          Print out of the functions
          pre_ingest_qc: a text file that is output into the working dir
        """
        # initialize report
        report = {}
        # grab the dictionary from Gen3Submission
        dd = self.sub.get_dictionary_all()

        batch_name = ppb[2] + "_" + ppb[3]

        case_df = pd.read_csv(
            batch_tsvs["node_tsvs"]["case"], sep="\t", header=0, dtype=str
        )

        cids = list(set(case_df.submitter_id))

        print("Found {} unique submitter_ids in the case TSV.".format(len(cids)))
        report["len_case_ids"] = "Unique submitter_ids in case TSV: " + str(len(cids))

        for node in list(batch_tsvs["node_tsvs"]):
            # read in the node TSV
            print("\n{}".format(node))
            report[node] = []  # initialize the node in the report dictionary
            df = pd.read_csv(batch_tsvs["node_tsvs"][node], sep="\t", dtype=str)

            # check case IDs
            errors = self.check_case_ids(df, node, cids)
            report[node] += errors

            # check that the 'type' column (node ID) is present and correct
            errors = self.check_type_field(df, node)
            report[node] += errors

            # check for submitter_id column completeness and uniqueness
            errors = self.check_submitter_id(df, node)
            report[node] += errors

            # check for presence and completeness of link columns
            errors = self.check_links(df, node, dd)
            report[node] += errors

            # check for special characters in the TSV
            errors = self.check_special_chars(node, batch_tsvs)
            report[node] += errors

            # check for required property completeness
            errors = self.check_required_props(df, node, dd, exclude_props)
            report[node] += errors

            # check for missing data in non-requred properties; check if TSV has columns with all null values
            errors = self.check_completeness(df, node)
            report[node] += errors

            # Check for correct property value types; i.e., number properties are numbers, strings are strings, etc.
            errors = self.check_prop_types(df, node, dd, exclude_props)
            report[node] += errors

            if len(report[node]) == 0:
                msg = "No errors for '{}' TSV".format(node)
                report[node] = msg

            file_name = "pre_ingest_qc_report_" + batch_name + ".txt"

            report_out = report_output_dir + file_name

        with open(report_out, "w") as file:
            json.dump(report, file)

        return report

    ########################################################################
    ## Submission to the graph
    ########################################################################

    # Function to submit a new program
    def submit_program(self, ppb):
        prog = ppb[0]
        print(f"Submitting a new program: {prog} to the endpoint: {self._endpoint}.")
        try:
            sub_obj = self.sub.create_program(prog)
            return sub_obj
        except ValueError as ve:
            raise ValueError(str(ve))

    # Function to submit a new project
    def submit_project(self, ppb, project_node):
        prog = ppb[0]
        proj = ppb[1]
        print(f"Submitting a new project: {proj} to: {prog}.")
        try:
            sub_obj = self.sub.create_project(program=prog, json=project_node)
            print("Please ensure that access is granted to any new project!")
            return sub_obj
        except ValueError as ve:
            raise ValueError(str(ve))

    # Function to Submit planxton file type node to the graph
    def submit_file(self, ppb, file):
        """Submits a file at a file path with Gen3Submission OR a pandas dataframe with Expansion
        Args:
          ppb: The program/batch combination (str)
          file: The file type object (file_path or pd.DataFrame)
        Returns:
          sub_obj: The result of the sheepdog submission
        """
        pid = self.extract_pid(ppb)
        if isinstance(file, str):
            node_file_df = pd.read_csv(file, sep="\t")
            node_type = node_file_df["type"].iloc[0]
            print(f"Submitting a '{node_type}' type file to: {pid}.")
            try:
                sub_obj = self.sub.submit_file(project_id=pid, filename=file)
                return sub_obj
            except ValueError as ve:
                raise ValueError(str(ve))
        elif isinstance(file, pd.DataFrame):
            try:
                sub_obj = self.exp.submit_df(project_id=ppb)
                return sub_obj
            except ValueError as ve:
                raise ValueError(str(ve))

    # Function to submit a planxton record type node to the graph
    def submit_record(self, ppb, json):
        """Submits a json string or list of json strings with Gen3Submission
        Args:
          ppb: The program/batch combination (str)
          json: Either a single json object or list of json objects(str)
        Returns:
          sub_obj: The result of the sheepdog submission
        """
        prog = ppb[0]
        proj = ppb[1]
        # Check if json represents a single record or multiple records
        if isinstance(json, list):
            # Handle multiple records
            for record in json:
                record_type = record["type"]
                print(f"Submitting a {record_type} type record to: {ppb}.")
                try:
                    sub_obj = self.sub.submit_record(
                        program=prog, project=proj, json=record
                    )
                except ValueError as ve:
                    raise ValueError(str(ve))
        else:
            # Handle a single record
            record_type = json["type"]
            print(f"Submitting a {record_type} type record to: {ppb}.")
            try:
                self.sub.submit_record(program=prog, project=proj, json=json)
            except ValueError as ve:
                raise ValueError(str(ve))

    # Function for all submission to Sheepdog with Planxton
    def submit_to_graph(self, node_type, ppb, node):
        """Submits objects to the established MIDRC commons
        Args:
          node_type: Type of node to submit to the graph (str)
          ppb: The program/batch combination (str)
          node: The node object to submit (varies)

        Returns:
          sub_obj: The result of the sheepdog submission
        """
        if node_type == "program":
            sub_obj = self.submit_program(ppb=ppb)
        elif node_type == "project":
            sub_obj = self.submit_project(ppb=ppb, project_node=node)
        elif node_type == "file":
            sub_obj = self.submit_file(ppb=ppb, file_path=node)
        elif node_type == "record":
            sub_obj = self.submit_record(ppb=ppb, json=node)
        else:
            print(
                "Please specify a submission type: 'program', 'project', 'file', or 'record'."
            )
        return sub_obj

    # Function for submitting a list of nodes to Sheepdog with Planxton
    def submit_list_to_graph(self, ppb, nodes_dict, node_type="file"):
        """Submits a list of objects to the established MIDRC commons
        Args:
          node_types: The shared type of all the nodes in the list.  All nodes must share the same type. (str)
          pb: The program/batch combination (str)
          list_types: The type of objects submitted in the
          submission_order: A dictionary of nodes and their submission order
          node_dict: a dictionary containing the nodes with keys being node type and values being either file paths or dataframes (str, pd.DataFrame)
        """
        if not isinstance(nodes_dict, dict):
            return "Error: node_types must be a dictionary"
        sub_objs = []
        if self.check_submission_order(nodes_dict)[0]:
            for node in node_list:
                sub_obj = self.submit_to_graph(node_type, ppb, node[0])
                print(sub_obj)
                sub_objs.append(sub_obj)
            return sub_objs

    ########################################################################
    ########################################################################
    ##
    ##MIDRC Specific Functions
    ##
    ########################################################################
    ########################################################################

    ########################################################################
    ##Function for creating a Program and Project nodes
    ########################################################################

    def create_program_node(self, ppb):
        """Creates a program node for BDCat
        Args:
          sub (Gen3Submission): Gen3 Submission with authorization into the data commons
          program (str): Program for the node, for example: "topmed", "PCGC", etc.
        """
        program = ppb[0]
        prog_txt = """{
      "type": "program",
      "name": "%s"
      }""" % (
            program
        )
        prog_json = json.loads(prog_txt)
        return prog_json

    def create_project_node(self, ppb, full_name):
        """Creates a project node for BDCat
        Args:
          sub (Gen3Submission): Gen3 Submission with authorization into the data commons
          ppc (str): Program, Project and Consent String delimited by "-" and "_" respectively (ex. BioLINCC-MESA_c1)
          name (str): Name of the project.  This should only include the name and not abbreviation
          dbgap_ascnum (str): dbGaP acsencion number that includes the phs id, version, participant set number and consent number delimited by "." (ex. phs002910.v1.p1.c1)
        """
        program = ppb[0]
        proj = ppb[1]
        proj_txt = """{
      "type": "project",
      "code": "%s",
      "name": "%s",
      "dbgap_accession_number": "%s"
      }""" % (
            proj,
            full_name,
        )

        proj_json = json.loads(proj_txt)

        return proj_json

    ########################################################################
    ## Function for Creating a Dataset node
    ########################################################################
    def create_dataset_node(self, ppb):
        """Creates a dataset node for MIDRC
        Args:
          ppb (list): The ppb submission object for planxton_midrc
        """
        proj = ppb[1]
        org = ppb[2]
        batch_name = ppb[2] + "_" + ppb[3]

        dataset_txt = """{
      "type": "dataset",
      "data_contributor": "%s",
      "data_description": "%s",
      "projects": [
        {
          "code": "%s"
        }
      ],
      "submitter_id": "%s"
      }""" % (
            org,
            batch_name,
            proj,
            batch_name,
        )
        dataset_json = json.loads(dataset_txt)
        return dataset_json

    ########################################################################
    ## Function for Creating a Core Metadata Collection node
    ########################################################################
    def create_cmc_node(self, ppb):
        """Creates a Core Metadata Collection node for MIDRC
        Args:
          ppb (list): The ppb submission object for planxton_midrc
        """
        pid = self.extract_pid(ppb)
        batch_name = ppb[2] + "_" + ppb[3]
        proj = ppb[1]

        cmc_txt = """{
      "description": "Data from the %s study %s.",
      "submitter_id": "%s",
      "title": "%s",
      "project_id": "%s",
      "type": "core_metadata_collection",
      "projects": [
        {
          "code": "%s"
        }
      ]
    }""" % (
            pid,
            batch_name,
            batch_name,
            batch_name,
            pid,
            proj,
        )
        cmc_json = json.loads(cmc_txt)

        return cmc_json

    ########################################################################
    ## Function for Creating a Case node
    ########################################################################
    def create_case_node(self, ppb, tsvs):
        filename = tsvs["case"]

        df = pd.read_csv(filename, sep="\t", header=0, dtype=str)

        df["type"] = "case"

        if "datasets" in df:
            df.rename(columns={"datasets": "datasets.submitter_id"}, inplace=True)
        if "country_of_origin" in df:
            df.rename(
                columns={"country_of_origin": "country_of_residence"}, inplace=True
            )

        df["datasets.submitter_id"] = ppb[2] + "_" + ppb[3]

        df["race"] = df["race"].replace(
            "American Indian or Alaskan Native", "American Indian or Alaska Native"
        )
        df["race"] = df["race"].replace(
            "Native Hawaiian or Other Pacific Islander",
            "Native Hawaiian or other Pacific Islander",
        )

        # drop properties because 0 and 1 values cannot be determined as True or False
        df.loc[df["icu_indicator"] == "0", "icu_indicator"] = False
        df.loc[df["icu_indicator"] == "1", "icu_indicator"] = True

        df["sex"].replace("Unknown", "Not Reported", inplace=True)
        df["race"].replace("Unknown/Declined", "Not Reported", inplace=True)

        # print information on your case node
        print(
            f"Counts of 'age_at_index_gt89': {df.age_at_index_gt89.value_counts()} \n"
        )
        print(
            f"Count of 'age_at_index' = NaN: {df.loc[df.age_at_index.isna()].shape[0]} \n"
        )
        print(
            (
                """{
          Example ids:
          submitter_id: %s
          case_ids: %s
          """
            )
            % (df.submitter_id[0], df.case_ids[0])
        )
        print(f"Set of sexes: {list(set(df.sex))} \n")
        print(f"Set of races: {list(set(df.race))} \n")

        return df

    ########################################################################
    ## Function for Creating a Measurement node
    ########################################################################

    def create_measurement_node(self, ppb, tsvs):
        filename = tsvs["measurement"]

        df = pd.read_csv(filename, sep="\t", header=0, dtype=str)
        df["type"] = "measurement"

        if "cases" in df:
            df.rename(columns={"cases": "cases.submitter_id"}, inplace=True)

        df["cases.submitter_id"] = df["cases.submitter_id"].str.replace(
            "Measurement_", ""
        )
        df["submitter_id"] = df["submitter_id"].str.replace("Measurement_", "")
        df["case_ids"] = df["cases.submitter_id"]

        if "test_method" in df:
            df.drop(columns="test_method", inplace=True)
        if "conditions" in df:
            df.drop(columns="conditions", inplace=True)
        if "procedures" in df:
            df.drop(columns="procedures", inplace=True)
        if "days_to_respiratory_viral_panel" in df:
            df.drop(columns={"days_to_respiratory_viral_panel"}, inplace=True)
        if "respiratory_viral_panel_detected" in df:
            df.drop(columns={"respiratory_viral_panel_detected"}, inplace=True)
        if "respiratory_viral_panel_not_detected" in df:
            df.drop(columns={"respiratory_viral_panel_not_detected"}, inplace=True)
        return df

    ########################################################################
    ## Function for Creating a imaging study node
    ########################################################################

    def create_img_study_node(self, ppb, tsvs):
        filename = tsvs["imaging_study"]
        df = pd.read_csv(filename, sep="\t", header=0, dtype=str)

        df["type"] = "imaging_study"
        df["age_at_imaging"] = pd.to_numeric(df["age_at_imaging"])

        df["age_at_imaging"] = pd.to_numeric(df["age_at_imaging"])

        if (df["age_at_imaging"] > 89).any():
            raise ValueError("There are values in 'age_at_imaging' greater than 89.")

        if "cases" in df:
            df.rename(columns={"cases": "cases.submitter_id"}, inplace=True)

        df["cases.submitter_id"] = df["cases.submitter_id"].str.replace("Case_", "")
        df["case_ids"] = df["cases.submitter_id"]

        if df["case_ids"].isna().any():
            raise ValueError("There are missing values in 'case_ids'.")

        df["study_year_shifted"] = df["study_year_shifted"].replace(
            {"Yes": True, "No": False}
        )

        if "study_covid_status" in df:
            df.drop(columns="study_covid_status", inplace=True)
        if "series_count" in df:
            df.drop(columns="series_count", inplace=True)
        if "midrc_harmonized_study_description" in df:
            df.drop(columns="midrc_harmonized_study_description", inplace=True)
        if "loinc_contrast" in df:
            df.drop(columns="loinc_contrast", inplace=True)
        if "loinc_long_common_name" in df:
            df.drop(columns="loinc_long_common_name", inplace=True)
        if "loinc_method" in df:
            df.drop(columns="loinc_method", inplace=True)
        if "loinc_system" in df:
            df.drop(columns="loinc_system", inplace=True)
        if "study_location" in df:
            df.drop(columns="study_location", inplace=True)
        if "loinc_code" in df:
            df.drop(columns="loinc_code", inplace=True)
        if "study_year" in df:
            df.drop(columns="study_year", inplace=True)

        return df

    ########################################################################
    ## Function for Creating a Visit node
    ########################################################################

    def create_visit_node(self, ppb, tsvs):
        filename = tsvs["visit"]
        df = pd.read_csv(filename, sep="\t", header=0, dtype=str)

        df["type"] = "visit"
        if "cases" in df:
            df.rename(columns={"cases": "cases.submitter_id"}, inplace=True)
        df["submitter_id"] = df["submitter_id"].str.replace("Visit_", "")
        df["case_ids"] = df["cases.submitter_id"]

        return df

    ########################################################################
    ## Function for Creating a Condition node
    ########################################################################

    def create_condition_node(self, ppb, tsvs):
        filename = tsvs["condition"]
        df = pd.read_csv(filename, sep="\t", header=0, dtype=str)

        if "cases" in df:
            df.rename(columns={"cases": "cases.submitter_id"}, inplace=True)
        return df

    ########################################################################
    ## Function for preparing the imaging_series_file TSVs using the indexd zip packages
    ########################################################################

    def prep_img_ser_file(self, ppb, tsvs, index_dir):
        batch_name = ppb[2] + "_" + ppb[3]

        open_file = "{}/indexed_packages_open_{}.tsv".format(index_dir, batch_name)
        seq_file = "{}/indexed_packages_seq_{}.tsv".format(index_dir, batch_name)

        oi = pd.read_csv(open_file, sep="\t", header=0, dtype=str)
        print(oi.shape)
        si = pd.read_csv(seq_file, sep="\t", header=0, dtype=str)
        print(si.shape)

        i = pd.concat([oi, si], ignore_index=True)
        assert len(i) == (len(oi) + len(si))

        print(f"Example of the file_name column: {i['file_name'][0]}/n")

        i["case_ids"] = i["file_name"].str.extract("(.*)\/.*\/.*\.zip")
        print(f"Example of the case_ids column: {i['case_ids'][0]}/n")
        i["study_uid"] = i["file_name"].str.extract(".*\/(.*)\/.*\.zip")
        print(f"Example of the study_uid column: {i['study_uid'][0]}/n")
        i["series_uid"] = i["file_name"].str.extract(".*\/(.*)\.zip")
        print(f"Example of the series_uid column: {i['series_uid'][0]}/n")

        if "guid" in i:
            i.rename(columns={"guid": "object_id"}, inplace=True)
        if "md5" in i:
            i.rename(columns={"md5": "md5sum"}, inplace=True)
        if "size" in i:
            i.rename(columns={"size": "file_size"}, inplace=True)

        return i

    ########################################################################
    ## Function for preparing the imaging_series_file TSVs using the indexd zip packages
    ########################################################################

    def create_series_tsv_dict(self, ppb, path):
        """Given a directory path, all series files will be made into a dictionary
        Args:
          path: Directory path that contains the series tsv files.  They must contain '*_series_{}_{}*'.format(org,batch) to be detected
        Return:
          series_dict: A dictionary containing all series file paths and types from the give directory
        """
        batch_name = ppb[2] + "_" + ppb[3]

        # Construct the pattern to search for files
        path_pattern = os.path.join(path, "*_series_{}*".format(batch_name))

        # Use glob to find all files that match the pattern in the specified directory
        series_tsvs = glob.glob(path_pattern)

        # Compile the regex pattern
        series_regex = re.compile(r"(.*_series)_{}.*".format(batch_name))

        # Create a dictionary with the last part of the file names as keys and the full paths as values
        series_dict = {
            os.path.basename(series_regex.match(i).group(1)): i
            for i in series_tsvs
            if series_regex.match(i)
        }

        return series_dict

    def create_series_tsvs(self, ppb, path, series_dict):
        """
        Cleans and creates image series submission node files given a dictionary of original files

        Args:
          ppb (list): The ppb submission object for planxton_midrc
          path (str): The directory path to write the submission tsvs to
          series_dict(dict): a dictionary of image series files to be cleaned

        Returns:
          Nothing, but it will save new tsv files for each that are either ready to submit or contain missing guids
        """
        batch_name = ppb[2] + "_" + ppb[3]

        for node in series_dict:
            print("\n\tProcessing {}: {}".format(node, series_dict[node]))

            df = pd.read_csv(series_tsvs[node], sep="\t", header=0, dtype=str)

            # Cleaning steps
            if "case_ids" in df:
                df["case_ids"] = df.case_ids.str.replace("Case_", "")
                display(df.case_ids[0])
            if "pixel_spacing" in df:
                df["pixel_spacing"] = df.pixel_spacing.str.replace("\\", ",")
                display(df.pixel_spacing[0])
            if "imager_pixel_spacing" in df:
                df["imager_pixel_spacing"] = df.pixel_spacing.str.replace("\\", ",")
                display(df.imager_pixel_spacing[0])
            if "scanning_sequence" in df:
                df["scanning_sequence"] = df.scanning_sequence.str.replace("\\", ",")
                display(df.scanning_sequence[0])
            if "lossy_image_compression" in df:
                df.loc[
                    df["lossy_image_compression"] == "0.0", "lossy_image_compression"
                ] = "00"
                df.loc[
                    df["lossy_image_compression"] == "0", "lossy_image_compression"
                ] = "00"
                df.loc[
                    df["lossy_image_compression"] == "1.0", "lossy_image_compression"
                ] = "01"
                df.loc[
                    df["lossy_image_compression"] == "1", "lossy_image_compression"
                ] = "01"
                display(list(set(df.lossy_image_compression)))
            if "series_description" in df:
                df["series_description"] = df["series_description"].str.replace(
                    "²", "^2"
                )

            # creating new columns
            df["type"] = "{}_file".format(node)
            display(list(set(df.type)))
            df["data_format"] = "DCM"
            df["data_type"] = "DICOM"
            if "modality" in df:
                df["data_category"] = df["modality"]
            display(list(set(df.data_category)))
            df["core_metadata_collections.submitter_id"] = batch_name

            # special case cleaning
            if node == "ct_series":
                df.loc[
                    df["contrast_bolus_agent"] == "120.0", "contrast_bolus_agent"
                ] = "120.00"

            if (
                node in ["dx_series", "cr_series", "ct_series"]
                and "contrast_bolus_agent_number" in df
            ):
                df.drop(columns="contrast_bolus_agent_number", inplace=True)

            if "radiography_exams.submitter_id" in df:
                df.rename(
                    columns={
                        "radiography_exams.submitter_id": "imaging_studies.submitter_id"
                    },
                    inplace=True,
                )
            elif "ct_scans.submitter_id" in df:
                df.rename(
                    columns={"ct_scans.submitter_id": "imaging_studies.submitter_id"},
                    inplace=True,
                )
            elif "mr_exams.submitter_id" in df:
                df.rename(
                    columns={"mr_exams.submitter_id": "imaging_studies.submitter_id"},
                    inplace=True,
                )
            elif "mr_exams.submitter_id" in df:
                df.rename(
                    columns={"mr_exams.submitter_id": "imaging_studies.submitter_id"},
                    inplace=True,
                )
            display(df["imaging_studies.submitter_id"][0])

            # checks
            series_uids = list(set(df.series_uid))
            print(
                "\t\tThe {} TSV contains {} records and {} unique series UIDs.".format(
                    node, len(df), len(series_uids)
                )
            )
