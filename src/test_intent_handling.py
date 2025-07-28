#!/usr/bin/env python3
"""
Comprehensive test suite for intent handling in VobChat.
Tests various query patterns to ensure all intents are extracted correctly.
"""

import unittest
from typing import List, Set, Dict, Tuple
from collections import Counter

from vobchat.intent_handling import extract_intent, AssistantIntent
from langchain_core.messages import HumanMessage


class TestIntentHandling(unittest.TestCase):
    """Test suite for intent extraction functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.maxDiff = None  # Show full diffs in assertions

    def _extract_intents(self, query: str) -> List[str]:
        """Helper to extract intent names from a query."""
        result = extract_intent(query, [HumanMessage(content=query)])
        return [intent.intent.value for intent in result.intents]
    
    def _extract_full_intents(self, query: str) -> List[Dict]:
        """Helper to extract full intent objects from a query."""
        result = extract_intent(query, [HumanMessage(content=query)])
        return [intent.model_dump() for intent in result.intents]

    def _assert_intents_match(self, query: str, expected_intents: List[str], 
                              expected_places: List[str] = None, 
                              expected_theme: str = None):
        """Assert that extracted intents match expectations."""
        actual_intents = self._extract_intents(query)
        full_intents = self._extract_full_intents(query)
        
        # Check intent types match (order-independent)
        self.assertCountEqual(actual_intents, expected_intents, 
                              f"Intent mismatch for query: '{query}'")
        
        # Check place names if provided
        if expected_places is not None:
            actual_places = [
                intent['arguments'].get('place', '').lower() 
                for intent in full_intents 
                if intent['intent'] == AssistantIntent.ADD_PLACE
            ]
            self.assertCountEqual(actual_places, [p.lower() for p in expected_places],
                                  f"Place mismatch for query: '{query}'")
        
        # Check theme if provided
        if expected_theme is not None:
            theme_intents = [
                intent['arguments'].get('theme_query', '').lower()
                for intent in full_intents
                if intent['intent'] == AssistantIntent.ADD_THEME
            ]
            self.assertTrue(any(expected_theme.lower() in theme for theme in theme_intents),
                            f"Theme '{expected_theme}' not found in query: '{query}'")

    def test_basic_stats_patterns(self):
        """Test basic patterns: [theme] stats for [places]"""
        test_cases = [
            ("population stats for portsmouth and newport", 
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['portsmouth', 'newport'], 'population'),
            
            ("housing data for london",
             ['AddPlace', 'AddTheme'],
             ['london'], 'housing'),
            
            ("employment statistics for glasgow and edinburgh",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['glasgow', 'edinburgh'], 'employment'),
            
            ("education data for birmingham and leeds",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['birmingham', 'leeds'], 'education'),
            
            ("crime stats for bristol and cambridge",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['bristol', 'cambridge'], 'crime'),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                self._assert_intents_match(query, expected_intents, 
                                           expected_places, expected_theme)

    def test_polite_request_patterns(self):
        """Test patterns with polite prefixes like 'please', 'can you', etc."""
        test_cases = [
            ("please get population stats for portsmouth and newport",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['portsmouth', 'newport'], 'population'),
            
            ("please show housing data for manchester",
             ['AddPlace', 'AddTheme'],
             ['manchester'], 'housing'),
            
            ("can you get employment statistics for york",
             ['AddPlace', 'AddTheme'],
             ['york'], 'employment'),
            
            ("could you show education data for oxford and cambridge",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['oxford', 'cambridge'], 'education'),
            
            ("would you please get crime stats for liverpool",
             ['AddPlace', 'AddTheme'],
             ['liverpool'], 'crime'),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                self._assert_intents_match(query, expected_intents,
                                           expected_places, expected_theme)

    def test_command_patterns(self):
        """Test imperative command patterns."""
        test_cases = [
            ("show population stats for cardiff",
             ['AddPlace', 'AddTheme'],
             ['cardiff'], 'population'),
            
            ("get housing data for swansea and newport",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['swansea', 'newport'], 'housing'),
            
            ("display employment statistics for dundee",
             ['AddPlace', 'AddTheme'],
             ['dundee'], 'employment'),
            
            ("fetch education data for aberdeen and inverness",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['aberdeen', 'inverness'], 'education'),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                self._assert_intents_match(query, expected_intents,
                                           expected_places, expected_theme)

    def test_multiple_places(self):
        """Test queries with multiple places."""
        test_cases = [
            ("population stats for london, manchester and birmingham",
             ['AddPlace', 'AddPlace', 'AddPlace', 'AddTheme'],
             ['london', 'manchester', 'birmingham'], 'population'),
            
            ("housing data for leeds, sheffield, liverpool and nottingham",
             ['AddPlace', 'AddPlace', 'AddPlace', 'AddPlace', 'AddTheme'],
             ['leeds', 'sheffield', 'liverpool', 'nottingham'], 'housing'),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                self._assert_intents_match(query, expected_intents,
                                           expected_places, expected_theme)

    def test_place_only_queries(self):
        """Test queries that only mention places."""
        test_cases = [
            ("add london", ['AddPlace'], ['london'], None),
            ("add manchester and birmingham", 
             ['AddPlace', 'AddPlace'], ['manchester', 'birmingham'], None),
            ("include oxford", ['AddPlace'], ['oxford'], None),
            ("show me cambridge", ['AddPlace'], ['cambridge'], None),
            ("where's bristol?", ['AddPlace'], ['bristol'], None),
            ("find newcastle", ['AddPlace'], ['newcastle'], None),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                self._assert_intents_match(query, expected_intents,
                                           expected_places, expected_theme)

    def test_theme_only_queries(self):
        """Test queries that only mention themes."""
        test_cases = [
            ("change theme to population", ['AddTheme'], None, 'population'),
            ("switch to housing theme", ['AddTheme'], None, 'housing'),
            ("use employment statistics", ['AddTheme'], None, 'employment'),
            ("set theme to education", ['AddTheme'], None, 'education'),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                self._assert_intents_match(query, expected_intents,
                                           expected_places, expected_theme)

    def test_special_intents(self):
        """Test special intent patterns."""
        test_cases = [
            ("show my current selection", ['ShowState']),
            ("what have I selected?", ['ShowState']),
            ("list all themes", ['ListThemes']),
            ("what themes are available?", ['ListThemes']),
            ("start over", ['Reset']),
            ("remove the theme", ['RemoveTheme']),
            ("remove london", ['RemovePlace']),
            ("tell me about manchester", ['PlaceInfo']),
            ("what is the population theme?", ['DescribeTheme']),
        ]
        
        for query, expected_intents in test_cases:
            with self.subTest(query=query):
                actual_intents = self._extract_intents(query)
                self.assertCountEqual(actual_intents, expected_intents,
                                      f"Intent mismatch for query: '{query}'")

    def test_complex_queries(self):
        """Test complex queries with multiple intent types."""
        test_cases = [
            # These should extract both place and theme intents
            ("I want to see population data, please add london and manchester",
             ['AddPlace', 'AddPlace', 'AddTheme'],
             ['london', 'manchester'], 'population'),
            
            ("housing stats for my selected places and also add birmingham",
             ['AddPlace', 'AddTheme'],
             ['birmingham'], 'housing'),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                self._assert_intents_match(query, expected_intents,
                                           expected_places, expected_theme)

    def test_edge_cases(self):
        """Test edge cases and potential problem patterns."""
        test_cases = [
            # Queries with "for" but no theme word before places
            ("stats for london", ['AddPlace', 'AddTheme'], ['london'], None),
            ("data for manchester and liverpool", 
             ['AddPlace', 'AddPlace', 'AddTheme'], ['manchester', 'liverpool'], None),
            
            # Queries with theme words but no explicit stats/data
            ("population in london", ['AddPlace', 'AddTheme'], ['london'], 'population'),
            ("housing for manchester", ['AddPlace', 'AddTheme'], ['manchester'], 'housing'),
        ]
        
        for query, expected_intents, expected_places, expected_theme in test_cases:
            with self.subTest(query=query):
                # For edge cases, we're more lenient about exact matches
                actual_intents = self._extract_intents(query)
                # Check that we have at least the expected intent types
                for intent in expected_intents:
                    self.assertIn(intent, actual_intents,
                                  f"Missing {intent} in query: '{query}'")

    def test_consistency(self):
        """Test that the same query consistently returns the same intents."""
        query = "please get population stats for portsmouth and newport"
        expected_intents = ['AddPlace', 'AddPlace', 'AddTheme']
        
        # Run the same query multiple times
        results = []
        for i in range(5):
            intents = self._extract_intents(query)
            results.append(Counter(intents))
        
        # All results should be identical
        first_result = results[0]
        for i, result in enumerate(results[1:], 1):
            self.assertEqual(result, first_result,
                             f"Inconsistent results on run {i+1} for query: '{query}'")

    def test_postcode_handling(self):
        """Test postcode recognition."""
        test_cases = [
            ("SW1A 1AA", ['AddPlace']),
            ("show data for M1 1AE", ['AddPlace']),
            ("population stats for OX1 3QD", ['AddPlace', 'AddTheme']),
        ]
        
        for query, expected_intents in test_cases:
            with self.subTest(query=query):
                actual_intents = self._extract_intents(query)
                # Check intent types match
                self.assertCountEqual(actual_intents, expected_intents,
                                      f"Intent mismatch for postcode query: '{query}'")
                
                # Verify postcode was extracted
                full_intents = self._extract_full_intents(query)
                add_place_intents = [i for i in full_intents 
                                     if i['intent'] == AssistantIntent.ADD_PLACE]
                if add_place_intents:
                    # Check that at least one has a postcode argument
                    has_postcode = any('postcode' in i['arguments'] 
                                       for i in add_place_intents)
                    self.assertTrue(has_postcode,
                                    f"No postcode argument found for query: '{query}'")


def run_tests(verbose=True, output_file=None):
    """Run all tests and return results."""
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIntentHandling)
    
    # Run tests with file output if specified
    if output_file:
        import sys
        with open(output_file, 'w') as f:
            runner = unittest.TextTestRunner(stream=f, verbosity=2 if verbose else 1)
            result = runner.run(suite)
    else:
        runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
        result = runner.run(suite)
    
    # Return summary
    return {
        'total': result.testsRun,
        'passed': result.testsRun - len(result.failures) - len(result.errors),
        'failed': len(result.failures),
        'errors': len(result.errors),
        'success': result.wasSuccessful()
    }


if __name__ == '__main__':
    # Run tests when script is executed directly
    import datetime
    
    output_filename = f"test_intent_handling_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    print("Running VobChat Intent Handling Test Suite")
    print("=" * 60)
    print(f"Output will be saved to: {output_filename}")
    
    # Write header to file
    with open(output_filename, 'w') as f:
        f.write("VobChat Intent Handling Test Suite Results\n")
        f.write(f"Run at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
    
    # Run tests with file output
    results = run_tests(verbose=True, output_file=output_filename)
    
    # Append summary to file
    with open(output_filename, 'a') as f:
        f.write("\n" + "=" * 60 + "\n")
        f.write("Test Summary:\n")
        f.write(f"  Total tests: {results['total']}\n")
        f.write(f"  Passed: {results['passed']}\n")
        f.write(f"  Failed: {results['failed']}\n")
        f.write(f"  Errors: {results['errors']}\n")
        f.write(f"  Success: {'YES' if results['success'] else 'NO'}\n")
    
    # Also print summary to console
    print("\n" + "=" * 60)
    print(f"Test Summary:")
    print(f"  Total tests: {results['total']}")
    print(f"  Passed: {results['passed']}")
    print(f"  Failed: {results['failed']}")
    print(f"  Errors: {results['errors']}")
    print(f"  Success: {'YES' if results['success'] else 'NO'}")
    print(f"\nResults saved to: {output_filename}")