DATABRICKS_URL=https://adb-984752964297111.11.azuredatabricks.net
DATABRICKS_TOKEN=dapiff250eb386939361235b847b8558c018

echo "populating [~/.databrickscfg]"
> ~/.databrickscfg
echo "[DEFAULT]" >> ~/.databrickscfg
echo "host = $DATABRICKS_URL" >> ~/.databrickscfg
echo "token = $DATABRICKS_TOKEN" >> ~/.databrickscfg
echo "" >> ~/.databrickscfg
