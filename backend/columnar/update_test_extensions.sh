#!/bin/bash

# Update all test files to use .bytedb extension
for file in columnar/*test.go; do
    echo "Updating $file..."
    # Use sed to replace .db" with .bytedb"
    sed -i '' 's/\.db"/\.bytedb"/g' "$file"
done

echo "Done! All test files updated to use .bytedb extension"