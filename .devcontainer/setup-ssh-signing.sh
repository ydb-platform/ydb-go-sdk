#!/bin/bash
set -e

# Ensure SSH directory exists
mkdir -p ~/.ssh
chmod 700 ~/.ssh

# Set up SSH signing with GitHub CLI if logged in
if gh auth status &>/dev/null; then
	# Configure Git to use SSH for signing
	git config --global gpg.format ssh
	git config --global commit.gpgsign true

	# Create a new SSH key for signing if one doesn't exist
	if [ ! -f ~/.ssh/id_ed25519_signing ]; then
		ssh-keygen -t ed25519 -C "$(git config user.email)" -f ~/.ssh/id_ed25519_signing -N ""
	fi

	# Set the signing key
	git config --global user.signingkey ~/.ssh/id_ed25519_signing

	# Add the key to GitHub
	echo "Adding SSH key to GitHub for commit signing..."
	gh ssh-key add ~/.ssh/id_ed25519_signing.pub --title "Devcontainer Signing Key ($(hostname))" --type signing

	echo "SSH signing key setup complete!"
else
	echo "GitHub CLI not logged in. Please run 'gh auth login' first."
fi
