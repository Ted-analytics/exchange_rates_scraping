from dagster import IOManager, InputContext, OutputContext, ConfigurableIOManager
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import os

class ExchangeRateIOManager(ConfigurableIOManager):
    """IO Manager for handling exchange rate data between assets"""
    
    base_dir: str = "/tmp/exchange_rates"
    
    def _get_path(self, context) -> Path:
        """Generate path for storing asset data"""
        # Create a filename based on the asset key and timestamp
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{context.asset_key.path[-1]}_{ts}.json"
        
        # Ensure base directory exists
        os.makedirs(self.base_dir, exist_ok=True)
        
        return Path(self.base_dir) / filename

    def handle_output(self, context: OutputContext, obj):
        """Handle output data from an asset"""
        if obj is None:
            return
            
        path = self._get_path(context)
        
        try:
            # Convert data to serializable format
            if isinstance(obj, pd.DataFrame):
                data = {
                    "type": "dataframe",
                    "data": obj.to_dict(orient="records"),
                    "metadata": {
                        "columns": list(obj.columns),
                        "index": obj.index.tolist()
                    }
                }
            elif isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], pd.DataFrame):
                # Handle case where we have DataFrame with metadata
                data = {
                    "type": "enriched_dataframe",
                    "data": obj["data"].to_dict(orient="records"),
                    "metadata": {
                        "columns": list(obj["data"].columns),
                        "index": obj["data"].index.tolist(),
                        "update_time": obj.get("update_time").isoformat() if obj.get("update_time") else None,
                        "additional_info": {k: v for k, v in obj.items() if k != "data"}
                    }
                }
            else:
                # Handle other types of data
                data = {
                    "type": "other",
                    "data": obj if isinstance(obj, (dict, list)) else str(obj)
                }
            
            # Save to file
            with open(path, 'w') as f:
                json.dump(data, f)
            
            context.log.info(f"Saved output to {path}")
            
        except Exception as e:
            context.log.error(f"Failed to save output: {str(e)}")
            raise

    def load_input(self, context: InputContext):
        """Load input data for an asset"""
        # Find the latest file for this asset
        asset_files = list(Path(self.base_dir).glob(f"{context.upstream_output.asset_key.path[-1]}_*.json"))
        if not asset_files:
            return None
            
        latest_file = max(asset_files, key=lambda p: p.stat().st_mtime)
        
        try:
            # Load and deserialize data
            with open(latest_file, 'r') as f:
                data = json.load(f)
            
            if data["type"] == "dataframe":
                return pd.DataFrame.from_records(data["data"])
                
            elif data["type"] == "enriched_dataframe":
                update_time = (
                    datetime.fromisoformat(data["metadata"]["update_time"])
                    if data["metadata"].get("update_time")
                    else None
                )
                return {
                    "data": pd.DataFrame.from_records(data["data"]),
                    "update_time": update_time,
                    **data["metadata"].get("additional_info", {})
                }
                
            else:
                return data["data"]
                
        except Exception as e:
            context.log.error(f"Failed to load input: {str(e)}")
            raise

class DatabaseStateIOManager(ConfigurableIOManager):
    """IO Manager for handling database state and metadata"""
    
    base_dir: str = "/tmp/exchange_rates/db_state"
    
    def _get_path(self, context) -> Path:
        """Generate path for storing database state"""
        filename = f"{context.asset_key.path[-1]}_state.json"
        os.makedirs(self.base_dir, exist_ok=True)
        return Path(self.base_dir) / filename

    def handle_output(self, context: OutputContext, obj):
        """Handle database operation results"""
        if obj is None:
            return
            
        path = self._get_path(context)
        
        try:
            # Store operation metadata
            data = {
                "timestamp": datetime.now().isoformat(),
                "records_processed": obj if isinstance(obj, int) else len(obj),
                "status": "success"
            }
            
            with open(path, 'w') as f:
                json.dump(data, f)
                
            context.log.info(f"Saved database state to {path}")
            
        except Exception as e:
            context.log.error(f"Failed to save database state: {str(e)}")
            raise

    def load_input(self, context: InputContext):
        """Load database state information"""
        path = self._get_path(context)
        
        if not path.exists():
            return None
            
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            context.log.error(f"Failed to load database state: {str(e)}")
            raise

# Update pipeline definitions to use these IO managers
def get_io_manager_config():
    return {
        "exchange_rate_io_manager": ExchangeRateIOManager(
            base_dir="/tmp/exchange_rates"
        ),
        "db_state_io_manager": DatabaseStateIOManager(
            base_dir="/tmp/exchange_rates/db_state"
        )
    }