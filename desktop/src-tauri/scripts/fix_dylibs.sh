set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../.."

if [ "$TAURI_ENV_DEBUG" = "true" ]; then
  PROFILE="debug"
else
  PROFILE="release"
fi

# Fix dylibs for the main binary
BIN_PATH="$PROJECT_ROOT/target/$PROFILE/openbb-platform"

if [ ! -f "$BIN_PATH" ]; then
  echo "Binary not found at $BIN_PATH"
  exit 1
fi

LIBCRYPTO_PATH=$(otool -L "$BIN_PATH" | grep 'libcrypto.3.dylib' | awk '{print $1}')
LIBSSL_PATH=$(otool -L "$BIN_PATH" | grep 'libssl.3.dylib' | awk '{print $1}')

# Fix main binary references
install_name_tool -change \
  "$LIBCRYPTO_PATH" \
  "@executable_path/../Frameworks/libcrypto.3.dylib" \
  "$BIN_PATH"

install_name_tool -change \
  "$LIBSSL_PATH" \
  "@executable_path/../Frameworks/libssl.3.dylib" \
  "$BIN_PATH"

echo "Fixed dylibs for $PROFILE binary."

# Fix the bundled dylibs themselves
FRAMEWORKS_DIR="$PROJECT_ROOT/target/$PROFILE/bundle/macos/Open Data Platform.app/Contents/Frameworks"

if [ -d "$FRAMEWORKS_DIR" ]; then
  echo "Fixing bundled dylib references..."

  # Fix libssl.3.dylib's reference to libcrypto.3.dylib
  if [ -f "$FRAMEWORKS_DIR/libssl.3.dylib" ]; then
    LIBSSL_CRYPTO_PATH=$(otool -L "$FRAMEWORKS_DIR/libssl.3.dylib" | grep 'libcrypto.3.dylib' | awk '{print $1}')

    install_name_tool -change \
      "$LIBSSL_CRYPTO_PATH" \
      "@loader_path/libcrypto.3.dylib" \
      "$FRAMEWORKS_DIR/libssl.3.dylib"

    # Fix the install name of libssl itself
    install_name_tool -id "@rpath/libssl.3.dylib" "$FRAMEWORKS_DIR/libssl.3.dylib"

    echo "Fixed libssl.3.dylib"
  fi

  # Fix the install name of libcrypto
  if [ -f "$FRAMEWORKS_DIR/libcrypto.3.dylib" ]; then
    install_name_tool -id "@rpath/libcrypto.3.dylib" "$FRAMEWORKS_DIR/libcrypto.3.dylib"
    echo "Fixed libcrypto.3.dylib"
  fi
fi

echo "Successfully fixed all dylib references."
