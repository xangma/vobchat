#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from vobchat.intent_handling import extract_intent
from langchain_core.messages import HumanMessage

def test_intent_extraction():
    """Test intent extraction with various inputs"""
    
    test_cases = [
        "can you please show populations tats for portsmouth and newport?",
        "show population data for London and Manchester",  
        "add Birmingham and Leeds",
        "population stats for Portsmouth, Newport and Bristol",
        "show data for Portsmouth and Newport",
    ]
    
    for user_input in test_cases:
        print(f"Testing: '{user_input}'")
        print("=" * 60)
        
        # Create a minimal message history
        messages = [HumanMessage(content=user_input)]
        
        try:
            result = extract_intent(user_input, messages)
            
            print(f"Number of intents extracted: {len(result.intents)}")
            
            for i, intent in enumerate(result.intents):
                print(f"  Intent {i+1}: {intent.intent} - {intent.arguments}")
                
            print()
            
        except Exception as e:
            print(f"Error: {e}")
            print()
    
    print("SUMMARY:")
    print("Expected: Multiple AddPlace intents for each place mentioned")
    print("Actual: Only first place being extracted in most cases")

if __name__ == "__main__":
    test_intent_extraction()