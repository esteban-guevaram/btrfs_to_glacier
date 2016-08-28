#! /bin/bash
## Example script to use glacier common operations from the awscli
## - list vaults and jobs
## - upload a file (single and multipart) 
## - download archive and inventory

COMMON_PARAMS=( --account-id - --vault-name dummy_vault )

aws glacier list-vaults               --account-id - 
aws glacier describe-vault            ${COMMON_PARAMS[@]}

aws glacier upload-archive            ${COMMON_PARAMS[@]} --body archive_body --archive-description "to_glacier_single_part_upl"
aws glacier initiate-multipart-upload ${COMMON_PARAMS[@]} --archive-description "to_glacier_multipart_upl" --part-size 4194304
aws glacier list-multipart-uploads    ${COMMON_PARAMS[@]}

# range : chunk_len * (part-1) / chunk_len * part - 1
aws glacier upload-multipart-part     ${COMMON_PARAMS[@]} --body archive_body --range 'bytes 0-4194303/*' --upload-id $UPLOAD_ID
aws glacier list-parts                ${COMMON_PARAMS[@]} --output json --upload-id $UPLOAD_ID

# checksum = sha256 tree hash of all parts
aws glacier complete-multipart-upload ${COMMON_PARAMS[@]} --archive-size 12582912 --upload-id $UPLOAD_ID --checksum $CHECKSUM

aws glacier initiate-job              ${COMMON_PARAMS[@]} --job-parameters '{"Type":"inventory-retrieval", "Description":"retrieval_test"}'
aws glacier initiate-job              ${COMMON_PARAMS[@]} --job-parameters '{"Type":"archive-retrieval","ArchiveId":"$ARCHIVE_ID","Description":"multipart_download"}'

aws glacier list-jobs                 ${COMMON_PARAMS[@]}

aws glacier get-job-output            ${COMMON_PARAMS[@]} --job-id $JOB_RETR_ID inv_result.json
aws glacier get-job-output            ${COMMON_PARAMS[@]} --job-id $JOB_UPLD_ID inv_retrieve.json

