# sftp\_lambda\_integration

This module contains code for integrating AWS Lambda with an sFTP server to support MIDRC file exchange workflows.

---

## 📁 Contents

* `regenstrief_s3_sftp.py`: Lambda function triggered by S3 uploads to `s3://regenstrief-sftp-upload/`. It transfers the uploaded file to a remote sFTP server.
* `extractData.py`: CLI script to extract metadata using Gen3Submission, based on a token mapping (crosswalk) file.
* `archive.py`: Helper functions to package files into ZIP archives.
* `test_s3_sftp.py`: Test script that simulates an S3 upload event for local testing of the Lambda logic.

---

## 🛠️ Usage

### Extract Metadata and Archive Files

```bash
python extractData.py \
  -cf ~/RSNA_20230106/token_file_RSNA_20230105.txt \
  -qa RECORD_ID \
  --endpoint https://data.midrc.org/ \
  --creds ~/.gen3/credentials.json \
  --archive-path ~/RSNA_20230106.zip
```

* Reads a token (crosswalk) file
* Queries Gen3 for metadata using `RECORD_ID`
* Archives token and metadata CSVs into a single ZIP

### Test Lambda Logic
* Securely add all the environment variables in the test_script (DO NOT COMMIT any secrets)
```bash
python test_s3_sftp.py
```

Simulates an S3 event and invokes the `lambda_handler` in `regenstrief_s3_sftp.py`.

## 🚀 Future Work

* Automate Lambda deployment using GitHub Actions
* Parameterize remote sFTP configuration via environment variables during test.
