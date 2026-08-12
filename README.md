<p align="center">
  <img src="assets/banner.png" alt="Hermes Agent" width="100%">
</p>

# Hermes Agent ☤ (Personal Fork)
<p align="center">
  <a href="https://hermes-agent.nousresearch.com/">Hermes Agent</a> | <a href="https://hermes-agent.nousresearch.com/">Hermes Desktop</a>
</p>
<p align="center">
  <a href="https://hermes-agent.nousresearch.com/docs/"><img src="https://img.shields.io/badge/Docs-hermes--agent.nousresearch.com-FFD700?style=for-the-badge" alt="Documentation"></a>
  <a href="https://discord.gg/NousResearch"><img src="https://img.shields.io/badge/Discord-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="Discord"></a>
  <a href="https://github.com/NousResearch/hermes-agent/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License: MIT"></a>
  <a href="https://nousresearch.com"><img src="https://img.shields.io/badge/Built%20by-Nous%20Research-blueviolet?style=for-the-badge" alt="Built by Nous Research"></a>
  <a href="README.zh-CN.md"><img src="https://img.shields.io/badge/Lang-中文-red?style=for-the-badge" alt="中文"></a>
  <a href="README.ur-pk.md"><img src="https://img.shields.io/badge/Lang-اردو-green?style=for-the-badge" alt="اردو"></a>
  <a href="README.es.md"><img src="https://img.shields.io/badge/Lang-Español-orange?style=for-the-badge" alt="Español"></a>
</p>

> **Fork Disclaimer:** This is a personal fork of [NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent). This repository is maintained independently to run and test customizations I want to use in my daily workflows, and potentially see upstream.

## Personal Customizations

This fork includes the following specific capabilities not present in the upstream repository:

- **Enhanced `/update` Workflow:** A custom AGY/Gemini-routed upgrade path with upstream sync, guarded commit/push logic, progress streaming, tmux watcher/lifecycle hardening, and automatic gateway restart (`systemctl --user restart hermes-gateway`).
- **WhatsApp Bridge Improvements:** Enhancements to the native/cloud WhatsApp bridge, including better message delivery and progress status handling.
- **Azure Foundry Support:** Added provider and model-picker support for Azure Foundry.
- **Regression Coverage:** Focused tests designed to ensure the stability and safety of these customizations.

## Upstream Links

- **Original Repository:** [NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)
- **Official Documentation:** [hermes-agent.nousresearch.com](https://hermes-agent.nousresearch.com/)
- **Upstream Issues:** For general bugs affecting the core engine, please refer to the [upstream issue tracker](https://github.com/NousResearch/hermes-agent/issues).

- **Fork Issues:** For bugs or enhancements specific to the custom workflows in this repository, please use [this fork's issue tracker](https://github.com/highamperage/hermes-agent/issues).

---

## License

Hermes Agent is built by [Nous Research](https://nousresearch.com) and released under the MIT License. All original attribution remains with the upstream authors.
