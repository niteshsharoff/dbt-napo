run_date=$(date "+%Y-%m-%d")

rm -r {{ local_dir }}/$run_date
mkdir -p {{ local_dir }}/$run_date && cd {{ local_dir }}/$run_date

gsutil cp {{ gcs_uri }} .
files=(*)
filename="${files[0]}"

SSHPASS={{ password }} \
sshpass -e sftp -o StrictHostKeyChecking=no -P {{ port }} '{{ user }}'@{{ host }} <<< "put $filename {{ remote_dir }}/$filename"