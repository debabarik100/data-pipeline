from __future__ import absolute_import

import logging

from salecounttracker import BreakfastItemSale

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  BreakfastItemSale.run()
