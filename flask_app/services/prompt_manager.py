import os
import glob
import random

subjects = [
    # solo_female
    {"subject_type": "solo_female", "hair": "long, flowing, brunette"},
    {"subject_type": "solo_female", "hair": "short, pixie cut, blonde"},
    {"subject_type": "solo_female", "hair": "braided, shoulder length, black"},
    {"subject_type": "solo_female", "hair": "tied back, ponytail, red"},
    {"subject_type": "solo_female", "hair": "wavy, loose, auburn"},
    {"subject_type": "solo_female", "hair": "straight, long, dark brown"},
    {"subject_type": "solo_female", "hair": "bob cut, chestnut"},
    {"subject_type": "solo_female", "hair": "curly, tied up, light brown"},
    {"subject_type": "solo_female", "hair": "layered, shoulder length, blonde"},
    {"subject_type": "solo_female", "hair": "straight, medium length, black"},
    {"subject_type": "solo_female", "hair": "bob cut, red"},
    {"subject_type": "solo_female", "hair": "curly, long, dark brown"},
    {"subject_type": "solo_female", "hair": "wavy, shoulder length, light brown"},
    {"subject_type": "solo_female", "hair": "short, undercut, platinum"},
    {"subject_type": "solo_female", "hair": "long, curly, silver"},
    {"subject_type": "solo_female", "hair": "medium length, wavy, pink"},
    {"subject_type": "solo_female", "hair": "asymmetrical bob, blue"},
    {"subject_type": "solo_female", "hair": "long, straight, green"},
    # solo_male
    {"subject_type": "solo_male", "hair": "curly, medium length, dark blonde"},
    {"subject_type": "solo_male", "hair": "buzz cut, black"},

]


class PromptManager:
    def __init__(self, prompts_folder: str = "prompts"):
        self.prompts_folder = prompts_folder
        self.prompts_cache = {}
        self.subjects = subjects
        self.load_all_prompts()

    def load_all_prompts(self) -> None:
        if not os.path.exists(self.prompts_folder):
            os.makedirs(self.prompts_folder, exist_ok=True)
            return
        prompt_files = glob.glob(os.path.join(self.prompts_folder, "*.txt"))
        for file_path in prompt_files:
            filename = os.path.basename(file_path)
            key = filename.replace('.txt', '').lower().replace(' ', '_').replace('-', '_')
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    self.prompts_cache[key] = content
            except Exception:
                continue

    def normalize_key(self, text: str) -> str:
        if not text:
            return ""
        return text.lower().replace(' ', '_').replace('-', '_')

    def get_random_subject_description(self) -> str:
        if not self.subjects:
            return ""
        subject = random.choice(self.subjects)
        parts = []
        if subject.get("subject_type"):
            parts.append(f"Subject: {subject['subject_type']}")
        if subject.get("hair"):
            parts.append(f"Hair: {subject['hair']}")
        return "\n".join(parts)

    def get_prompt(self, vertical=None, category_name=None, sub_category_name=None, email_subject: str = "", formatted_highlights: str = ""):
        candidates = []
        if sub_category_name:
            candidates.append(self.normalize_key(sub_category_name))
        if category_name:
            candidates.append(self.normalize_key(category_name))
        if vertical:
            candidates.append(self.normalize_key(vertical))
        candidates.append('default')

        for candidate in candidates:
            if candidate in self.prompts_cache:
                prompt = self.prompts_cache[candidate]
                prompt = prompt.replace('{email_subject}', email_subject or '')
                prompt = prompt.replace('{formatted_highlights}', formatted_highlights or '')
                if '{subject}' in prompt:
                    prompt = prompt.replace('{subject}', self.get_random_subject_description())
                return prompt, candidate
        return f"Create a high-quality promotional image for: {email_subject}", "fallback"
