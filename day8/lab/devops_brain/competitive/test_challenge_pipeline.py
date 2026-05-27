from datetime import datetime, timedelta

def filter_recent_transactions(transactions, cutoff_date):
    return [t for t in transactions if t['date'] > cutoff_date]

def apply_silver_rules(transactions):
    return [t for t in transactions if t.get('transaction_id') and t.get('amount') >= 0]

import pytest

def test_filter_recent_transactions_boundary():
    transactions = [
        {'date': datetime.now() - timedelta(days=1), 'amount': 100},
        {'date': datetime.now(), 'amount': 200},
        {'date': datetime.now() + timedelta(days=1), 'amount': 300},
    ]
    cutoff_date = datetime.now()
    result = filter_recent_transactions(transactions, cutoff_date)
    assert len(result) == 2
    assert result[0]['date'] == transactions[1]['date']
    assert result[1]['date'] == transactions[2]['date']

def test_apply_silver_rules_none_transaction_id():
    transactions = [
        {'transaction_id': 1, 'amount': 100},
        {'transaction_id': None, 'amount': 200},
        {'transaction_id': 3, 'amount': 300},
    ]
    result = apply_silver_rules(transactions)
    assert len(result) == 2
    assert result[0]['transaction_id'] == 1
    assert result[1]['transaction_id'] == 3

def test_apply_silver_rules_negative_amount():
    transactions = [
        {'transaction_id': 1, 'amount': 100},
        {'transaction_id': 2, 'amount': -200},
        {'transaction_id': 3, 'amount': 300},
    ]
    result = apply_silver_rules(transactions)
    assert len(result) == 2
    assert result[0]['transaction_id'] == 1
    assert result[1]['transaction_id'] == 3