DATABRICKS_URL=https://XXX.azuredatabricks.net
DATABRICKS_TOKEN=XXX

echo "populating [~/.databrickscfg]"
> ~/.databrickscfg
echo "[DEFAULT]" >> ~/.databrickscfg
echo "host = $DATABRICKS_URL" >> ~/.databrickscfg
echo "token = $DATABRICKS_TOKEN" >> ~/.databrickscfg
echo "" >> ~/.databrickscfg
