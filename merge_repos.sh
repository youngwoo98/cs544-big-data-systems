#!/bin/bash

# Set the base repository (Ensure you've cloned it)
BASE_REPO_DIR="cs544-big-data-systems"

# List of repositories to merge
REPO_NAMES=("cs544-p1" "cs544-p2" "cs544-p3" "cs544-p4" "cs544-p5" "cs544-p6" "cs544-p7" "cs544-p8")
GIT_USER="youngwoo98"  # Change to your GitHub username
BRANCH="main"  # Change to 'master' if needed

# Ensure we are in the parent directory
if [ ! -d "$BASE_REPO_DIR" ]; then
    echo "Error: Directory $BASE_REPO_DIR not found."
    exit 1
fi

cd "$BASE_REPO_DIR" || exit

# Ensure the repo has at least one commit
if ! git rev-parse --verify HEAD >/dev/null 2>&1; then
    echo "Creating initial commit..."
    touch .gitkeep
    git add .gitkeep
    git commit -m "Initial commit"
fi

# Ensure we are on the correct branch
git checkout -B "$BRANCH"

# Loop through each repository and merge it using git subtree
for REPO in "${REPO_NAMES[@]}"; do
    echo "Processing $REPO..."

    # Check if repository exists
    REPO_URL="git@github.com:$GIT_USER/$REPO.git"
    if ! git ls-remote "$REPO_URL" >/dev/null 2>&1; then
        echo "Error: Repository $REPO not found or inaccessible!"
        continue
    fi

    # Add the remote repository
    git remote add "$REPO" "$REPO_URL"

    # Fetch the repository (retry logic in case of network issues)
    echo "Fetching $REPO..."
    git fetch "$REPO" || { echo "Failed to fetch $REPO"; continue; }

    # Check if the repository has a main or master branch
    REMOTE_BRANCH="main"
    if ! git ls-remote --exit-code --heads "$REPO" main >/dev/null; then
        REMOTE_BRANCH="master"
    fi

    echo "Merging $REPO into $BASE_REPO_DIR/$REPO (using branch $REMOTE_BRANCH)..."
    git subtree add --prefix="$REPO" "$REPO" "$REMOTE_BRANCH"  # Removed --squash to keep commit history

    # Remove the remote to keep the repo clean
    git remote remove "$REPO"

    echo "Merged $REPO successfully."
done

# Show the final structure
echo "Final repository structure:"
ls -R

# Commit and push changes
git add .
git commit -m "Merged cs544-p1 to cs544-p8 into $BASE_REPO_DIR"
git push origin "$BRANCH"

echo "All repositories merged successfully!"

