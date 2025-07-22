# File-Based Prompt System for Image Generation

This updated system loads prompts from text files instead of hardcoded variables, making it much easier to manage and update prompts.

## How It Works

The system now uses a `PromptManager` class that:

1. **Loads prompts from files**: Reads all `.txt` files from the `prompts/` folder
2. **Matches by priority**: Finds the best prompt based on:
   - `sub_category_name` (highest priority)
   - `category_name` 
   - `vertical`
   - `default` (fallback)
3. **Replaces placeholders**: Automatically substitutes placeholders with actual data:
   - `{email_subject}` → Deal title
   - `{formatted_highlights}` → Key deal features (bullet points)  
   - `{subject}` → Random person description for travel/lifestyle images

## File Naming Convention

Prompt files should be named using lowercase with underscores:

- `spa.txt` → matches sub_category_name = "Spa"
- `beauty.txt` → matches category_name = "Beauty"  
- `national.txt` → matches vertical = "NATIONAL"
- `travel.txt` → matches vertical = "TRAVEL"
- `local.txt` → matches vertical = "LOCAL"
- `default.txt` → fallback for any unmatched deals

Special characters are automatically converted:
- Spaces become underscores: "Spas & Country House" → `spas_and_country_house.txt`
- Ampersands become "and": "Beauty & Wellness" → `beauty_and_wellness.txt`

## Placeholder System

The system supports three flexible placeholders that can be used in any combination:

### `{email_subject}`
- Replaced with the actual deal title
- Example: "Luxury Spa Day for 2 with Treatments"
- Used in: All prompt types

### `{formatted_highlights}` 
- Replaced with key deal features as bullet points
- Example:
  ```
  • Full body massage
  • Use of spa facilities  
  • Prosecco and light refreshments
  ```
- Used in: Prompts that need to emphasize specific features

### `{subject}`
- Replaced with a random person description for lifestyle imagery
- Example:
  ```
  Subject: solo_female
  Pose: natural, candid — walking, gazing, appreciating view
  Look: stylish, relaxed, immersed in the environment
  Attire: seasonal and appropriate to destination, subtly fashionable
  Hair: long, flowing, brunette
  ```
- Used in: Travel and lifestyle prompts

**Flexible Usage**: Prompts can use all three, just one, or any combination. Unused placeholders are simply ignored.

## Adding New Prompts

1. Create a new `.txt` file in the `prompts/` folder
2. Name it after the category/subcategory you want to target
3. Write your prompt using any combination of placeholders: `{email_subject}`, `{formatted_highlights}`, `{subject}`
4. Run the notebook - it will automatically load the new prompt

## Example Prompts Included

The system comes with these example prompts:

- **`spa.txt`**: Detailed spa-specific imagery requirements (uses `{email_subject}`)
- **`local.txt`**: General local deals prompt (uses `{email_subject}`)
- **`national.txt`**: Product photography focused prompt (uses `{email_subject}`)
- **`travel.txt`**: Travel magazine style collage prompt (uses `{email_subject}`)
- **`travel_with_subject.txt`**: Travel with person descriptions (uses `{email_subject}`, `{subject}`, `{formatted_highlights}`)
- **`beauty.txt`**: Beauty/wellness focused (uses `{email_subject}`, `{formatted_highlights}`)
- **`default.txt`**: Basic fallback prompt (uses `{email_subject}`)

## Key Changes from Original

1. **No more JSON**: All prompts are now plain text (easier to read/edit)
2. **No hardcoded logic**: The complex if/else logic is replaced with simple file matching
3. **Easy updates**: Just edit text files instead of notebook code
4. **Better tracking**: Results now include `prompt_source` showing which file was used
5. **Automatic loading**: Files are loaded once at startup and cached for performance

## File Structure

```
project/
├── generate_variants_file_based.ipynb  # Main notebook
├── prompts/                           # Prompt files folder
│   ├── spa.txt
│   ├── local.txt
│   ├── national.txt
│   ├── travel.txt
│   └── default.txt
└── README_file_based_prompts.md       # This file
```

## Usage Example

```python
# The PromptManager automatically loads prompts on startup
prompt_manager = PromptManager()

# Get a prompt for a spa deal with all placeholders
prompt, source = prompt_manager.get_prompt(
    vertical="LOCAL",
    category_name="Beauty", 
    sub_category_name="Spa",
    email_subject="Luxury Spa Day for 2",
    formatted_highlights="• Full body massage\n• Use of spa facilities\n• Prosecco included"
)

# This would use spa.txt and replace all placeholders
print(f"Using prompt from: {source}")  # Output: "spa"

# For travel prompts using {subject}, a random person description is automatically selected
travel_prompt, source = prompt_manager.get_prompt(
    vertical="TRAVEL",
    category_name="Travel",
    email_subject="Paris City Break",
    formatted_highlights="• 3 nights hotel\n• Breakfast included\n• City center location"
)
# If the prompt contains {subject}, it gets a random person like:
# "Subject: solo_female, Pose: natural candid, Hair: long flowing brunette" etc.
```

## Benefits

- **Easier maintenance**: Edit prompts without touching code
- **Version control**: Track prompt changes in git
- **Collaboration**: Non-technical team members can edit prompts
- **Testing**: Easy to A/B test different prompt versions
- **Organization**: Clear separation of prompts and logic
- **Flexibility**: Add new categories without code changes 