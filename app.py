from flask import Flask, request, jsonify
import os
import base64
import logging
from mbs import process_excel_data  # Your existing processing function

app = Flask(__name__)

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Folder where uploaded files will be stored
UPLOAD_FOLDER = os.path.join(BASE_DIR, "input", "non_cricket_input")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)  # Ensure folder exists

# Reference files
SKELETON_FILE = os.path.join(BASE_DIR, "input", "Skeleton Output.xlsx")
ER_CPRP_FILE = os.path.join(BASE_DIR, "input", "ER and CPRP Channels TV and Digital CTV-Mobile CPM.xlsx")
TVR_OUTPUT_FILE = os.path.join(BASE_DIR, "input", "TVR Output.xlsx")

# Logging
VERBOSE_LOGGING = True
log_level = logging.DEBUG if VERBOSE_LOGGING else logging.INFO
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === HELPERS ===
def is_base64_encoded(data):
  """Check if a string is valid Base64."""
  try:
      base64.b64decode(data)
      return True
  except Exception:
      return False

# === ROUTES ===
@app.route('/ping', methods=['GET'])
def ping():
  """Health check endpoint."""
  return jsonify({"status": "ok"}), 200

@app.route('/process_pager_excelfile', methods=['POST'])
def process_pager_excelfile():
  try:
      body = request.get_json()
      if VERBOSE_LOGGING:
          logger.debug("Request JSON body: %s", body)

      # Accept either a list of files or a single file object
      if "files" in body and isinstance(body["files"], list):
          files = body["files"]
      else:
          files = [body]

      if not files:
          msg = "No files to process"
          logger.error(msg)
          return jsonify({"error": msg}), 400

      file_map = {}

      for file_info in files:
          filename = file_info.get("xlsx-name")
          attach_body = file_info.get("attach-body")
          file_type = file_info.get("file-type")  # must be 'input_a' or 'input_b'

          logger.info("Processing file: %s of type %s", filename, file_type)

          if not attach_body or not filename or not file_type:
              msg = f"File '{filename}': Missing 'attach-body', 'xlsx-name', or 'file-type'"
              logger.error(msg)
              return jsonify({"error": msg}), 400

          content = attach_body.get("contentBytes")
          if not content:
              msg = f"File '{filename}': Missing 'contentBytes' in attach-body"
              logger.error(msg)
              return jsonify({"error": msg}), 400

          if not is_base64_encoded(content):
              msg = f"File '{filename}': Invalid base64 content"
              logger.error(msg)
              return jsonify({"error": msg}), 400

          decoded_bytes = base64.b64decode(content)
          input_path = os.path.join(UPLOAD_FOLDER, filename)

          with open(input_path, "wb") as f:
              f.write(decoded_bytes)
          logger.info("File saved successfully to: %s", input_path)

          file_map[file_type] = input_path

      # Ensure both required file types are present
      required_types = ["input_a", "input_b"]
      missing_types = [ft for ft in required_types if ft not in file_map]

      if missing_types:
          msg = f"Missing required files: {', '.join(missing_types)}"
          logger.error(msg)
          return jsonify({"error": msg}), 400

      # Process the Excel files
      output_file = os.path.join(UPLOAD_FOLDER, "Completed_Output.xlsx")
      process_excel_data(
          file_map["input_a"],
          file_map["input_b"],
          SKELETON_FILE,
          output_file
      )

      if not os.path.exists(output_file):
          raise FileNotFoundError(f"Output file not created: {output_file}")

      logger.info(f"Files processed successfully. Output saved to {output_file}")

      with open(output_file, "rb") as f:
          output_data = f.read()
      encoded_output = base64.b64encode(output_data).decode('utf-8')

      result = {
          "status": "success",
          "data": encoded_output,
          "output_filename": "Completed_Output.xlsx"
      }

      return jsonify(result)

  except Exception as e:
      logger.exception("An error occurred while processing the request.")
      return jsonify({"error": str(e)}), 500

# === UTILITY ===
def encode_file_to_base64(path):
  """Utility to encode a file to Base64."""
  with open(path, "rb") as f:
      return base64.b64encode(f.read()).decode('utf-8')

if __name__ == '__main__':
  # Example: Generate Base64 for testing
  base64_a = encode_file_to_base64(os.path.join("input", "non_cricket_input", "Non Cricket Input.xlsx"))
  base64_b = encode_file_to_base64(os.path.join("input", "TVR Output.xlsx"))
  print("Base64 A:", base64_a)
  print("Base64 B:", base64_b)

  port = int(os.environ.get("PORT", 8080))
  app.run(host='0.0.0.0', port=port, debug=True)