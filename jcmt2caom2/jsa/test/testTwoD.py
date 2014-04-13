#!/usr/bin/env python2.7
from __future__ import absolute_import

import math
import unittest

from jcmt2caom2.jsa.twod import TwoD


class testTwoD( unittest.TestCase):
    def testTwoDArithmetic(self):
        """
        Test basic properties
        """
        bl = TwoD(0.0, 0.0)
        br = TwoD(1.0, 0.0)
        tl = TwoD(0.0, 1.0)
        tr = TwoD(1.0, 1.0)
        
        self.assertFalse(bl == br)
        self.assertFalse(bl == tl)
        self.assertTrue(bl != tr)
        self.assertTrue(br != tl)
        self.assertNotEqual(br, tr)
        self.assertNotEqual(tl, tr)

        a = TwoD(tr)
        b = TwoD((1.0, 1.0))
        c = TwoD([1.0, 1.0])
        
        self.assertTrue(tr == a)
        self.assertTrue(tr == b)
        self.assertTrue(tr == c)

        self.assertEqual(br, bl + br)
        self.assertEqual(tr, tl + br)
        self.assertEqual(tr - tl - br, bl,
                         'diff is ' + str(tr - tl - br))
        
        self.assertEqual(2.0*tr, tr + tl + br)
        self.assertEqual(tr*2, tr + tl + br)
        
        self.assertEqual(tr.abs(), math.sqrt(2.0))
                
        xx = tl
        yy = tr
        self.assertTrue(xx is tl)
        self.assertTrue(yy is tr)
        
        xx = TwoD(tl)
        yy = TwoD(tr)
        self.assertFalse(xx is tl)
        self.assertFalse(yy is tr)
        self.assertEqual(xx, tl)
        self.assertEqual(yy, tr)
        
        xx.swap(yy)
        self.assertEqual(xx, tr,
                         'xx = ' + str(xx) + '  tr = ' + str(tr))
        self.assertEqual(yy, tl,
                         'yy = ' + str(yy) + '  tr = ' + str(tr))
        
        self.assertEqual(TwoD.cross(br, tl), 1.0)
        
        self.assertEqual((bl - tr).abs(), math.sqrt(2.0))
        
if __name__ == '__main__':
    unittest.main()
    
