# Installing the LandWorks Agent Team into OpenClaw

## Step 1 — Copy agent files to your OpenClaw workspace

Run these commands in Ubuntu to install all agents:

```bash
# Create the agents directory if it doesn't exist
mkdir -p ~/.openclaw/workspace/agents

# Copy all agent files
cp ALMA.md   ~/.openclaw/workspace/agents/ALMA.md
cp SPORT.md  ~/.openclaw/workspace/agents/SPORT.md
cp CADDY.md  ~/.openclaw/workspace/agents/CADDY.md
cp ARCH.md   ~/.openclaw/workspace/agents/ARCH.md
cp REX.md    ~/.openclaw/workspace/agents/REX.md
cp MEP.md    ~/.openclaw/workspace/agents/MEP.md
cp AGENTS.md ~/.openclaw/workspace/AGENTS.md
```

## Step 2 — Verify they're in place

```bash
ls ~/.openclaw/workspace/agents/
cat ~/.openclaw/workspace/AGENTS.md
```

You should see: ALMA.md, SPORT.md, CADDY.md, ARCH.md, REX.md, MEP.md

## Step 3 — Update your BOOTSTRAP.md (optional but recommended)

Add this to ~/.openclaw/workspace/BOOTSTRAP.md so OpenClaw knows to load the team:

```
## Active Agent Team

This workspace uses the LandWorks agent team. See AGENTS.md for the full roster.
All agents are available in ~/.openclaw/workspace/agents/

Load agents by referencing them with @AgentName in conversation.
Alma is the default project lead for all land development questions.
```

## Step 4 — Restart OpenClaw

```bash
openclaw gateway stop
openclaw gateway run
```

## Step 5 — Test the agents

In the OpenClaw chat, try:
- "Hey @Alma, I'm looking at a 10-acre parcel in Fuquay-Varina. Where do I start?"
- "@Sport, what stormwater permits do I need for a 15-acre subdivision in the Neuse watershed?"
- "@Caddy, how do I import Wake County LiDAR into Civil 3D?"

## Agent Invocation Tips

You can invoke agents naturally — OpenClaw will route to the right specialist:
- Business/deal questions → Alma responds
- Engineering/permits → Sport responds  
- CAD/drawings → Caddy responds
- Building design → Arch responds
- Construction/scheduling → Rex responds
- HVAC/electrical/plumbing → MEP responds

## Updating Agent Definitions

To update any agent's knowledge, simply edit the .md file and restart OpenClaw:
```bash
nano ~/.openclaw/workspace/agents/SPORT.md
openclaw gateway stop && openclaw gateway run
```
