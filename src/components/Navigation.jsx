// Modern navigation component with refined design
import React, { useState, useEffect, useRef } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';

export default function Navigation({ tabs, darkMode, onDarkModeToggle }) {
  const location = useLocation();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [openDropdown, setOpenDropdown] = useState(null);
  const dropdownRef = useRef(null);

  const currentPath = location.pathname;

  // Organize tabs into categories
  const topLevelLinks = [
    { id: 'alert-triage', name: 'Alert Triage' },
    { id: 'threat-intel', name: 'Threat Lookup' },
    { id: 'prompt-gallery', name: 'Prompts' },
    { id: 'email-headers', name: 'Header Analyzer' },
    { id: 'determination-generator', name: 'Determinations' },
    { id: 'soc-handoff', name: 'Shift Handoff' },
    { id: 'monthly-report', name: 'Monthly Report' }
  ];

  const engineering = {
    name: 'Engineering',
    items: [
      { id: 'kql-diff', name: 'KQL Diff Viewer' },
      { id: 'email-posture', name: 'Email Posture Check' }
    ]
  };

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setOpenDropdown(null);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Close dropdowns and mobile menu on route change
  useEffect(() => {
    setMobileMenuOpen(false);
    setOpenDropdown(null);
  }, [currentPath]);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        setOpenDropdown(null);
        if (mobileMenuOpen) {
          setMobileMenuOpen(false);
        }
      }

      if ((e.metaKey || e.ctrlKey) && e.key === 'h') {
        e.preventDefault();
        window.location.href = `/${topLevelLinks[0].id}`;
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [mobileMenuOpen, topLevelLinks]);

  const toggleDropdown = (dropdownKey) => {
    setOpenDropdown(openDropdown === dropdownKey ? null : dropdownKey);
  };

  const isPathInDropdown = (dropdown) => {
    return dropdown.items?.some(item => currentPath.startsWith(`/${item.id}`));
  };

  const DropdownMenu = ({ dropdownKey, dropdown }) => {
    const isOpen = openDropdown === dropdownKey;
    const isActive = isPathInDropdown(dropdown);
    const closeTimeoutRef = useRef(null);

    const handleMouseEnter = () => {
      // Clear any pending close timeout
      if (closeTimeoutRef.current) {
        clearTimeout(closeTimeoutRef.current);
        closeTimeoutRef.current = null;
      }
      setOpenDropdown(dropdownKey);
    };

    const handleMouseLeave = () => {
      // Add a small delay before closing to prevent accidental closes
      closeTimeoutRef.current = setTimeout(() => {
        setOpenDropdown(null);
      }, 150);
    };

    return (
      <div
        className="relative"
        ref={isOpen ? dropdownRef : null}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        <motion.button
          onClick={() => toggleDropdown(dropdownKey)}
          whileHover={{ y: -1 }}
          whileTap={{ y: 0 }}
          className={`relative px-3 py-1.5 text-sm font-medium whitespace-nowrap transition-all flex items-center gap-1.5 ${
            isActive
              ? darkMode
                ? 'text-blue-400'
                : 'text-blue-600'
              : isOpen
              ? darkMode
                ? 'text-white'
                : 'text-gray-900'
              : darkMode
              ? 'text-gray-400 hover:text-gray-200'
              : 'text-gray-600 hover:text-gray-900'
          }`}
          aria-expanded={isOpen}
          aria-haspopup="true"
        >
          <span>{dropdown.name}</span>
          <motion.span
            animate={{ rotate: isOpen ? 180 : 0 }}
            transition={{ duration: 0.2 }}
            className="text-xs"
          >
            ▼
          </motion.span>

          {/* Active underline */}
          {isActive && (
            <motion.div
              layoutId="activeDropdown"
              className={`absolute bottom-0 left-0 right-0 h-0.5 ${
                darkMode ? 'bg-blue-400' : 'bg-blue-600'
              }`}
              initial={false}
              transition={{ type: 'spring', stiffness: 500, damping: 30 }}
            />
          )}
        </motion.button>

        <AnimatePresence>
          {isOpen && (
            <motion.div
              initial={{ opacity: 0, y: -4 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -4 }}
              transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
              className={`absolute top-full left-0 mt-2 min-w-[220px] rounded-lg overflow-hidden z-50 border ${
                darkMode
                  ? 'bg-gray-800 border-gray-700'
                  : 'bg-white border-gray-200'
              }`}
            >
              <div className="py-1">
                {dropdown.items.map((item) => {
                  const isItemActive = currentPath.startsWith(`/${item.id}`);
                  return (
                    <Link
                      key={item.id}
                      to={`/${item.id}`}
                      className={`block px-4 py-2 text-sm transition-colors ${
                        isItemActive
                          ? darkMode
                            ? 'bg-gray-700 text-blue-400'
                            : 'bg-gray-100 text-blue-600'
                          : darkMode
                          ? 'text-gray-300 hover:bg-gray-700'
                          : 'text-gray-700 hover:bg-gray-50'
                      }`}
                    >
                      <span className="font-medium">{item.name}</span>
                    </Link>
                  );
                })}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    );
  };

  return (
    <nav
      className={`border-b ${
        darkMode
          ? 'bg-gray-900 border-gray-800'
          : 'bg-white border-gray-200'
      } sticky top-0 z-50`}
      aria-label="Main navigation"
    >
      <div className="max-w-7xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className={`text-xl font-semibold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              🛡️ ThreatDefender Operations Suite
            </h1>
            <p className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
              All your threats are belong to us
            </p>
          </div>

          <div className="flex items-center gap-3">
            {/* Dark Mode Toggle */}
            <motion.button
              whileHover={{ y: -1 }}
              whileTap={{ y: 0 }}
              onClick={onDarkModeToggle}
              className={`px-3 py-1.5 text-sm transition-colors ${
                darkMode
                  ? 'text-yellow-400 hover:text-yellow-300'
                  : 'text-gray-700 hover:text-gray-900'
              }`}
              aria-label={`Switch to ${darkMode ? 'light' : 'dark'} mode`}
            >
              {darkMode ? '☀️' : '🌙'}
            </motion.button>

            {/* Mobile Menu Toggle */}
            <motion.button
              whileTap={{ scale: 0.95 }}
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className={`md:hidden p-2 ${
                darkMode ? 'text-white hover:text-gray-300' : 'text-gray-900 hover:text-gray-700'
              }`}
              aria-label="Toggle mobile menu"
              aria-expanded={mobileMenuOpen}
            >
              <svg
                className="w-6 h-6"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                {mobileMenuOpen ? (
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                ) : (
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
                )}
              </svg>
            </motion.button>
          </div>
        </div>

        {/* Desktop Navigation */}
        <div className="hidden md:flex items-center gap-6" role="navigation">
          {/* Top Level Links */}
          {topLevelLinks.map((link) => {
            const isActive = currentPath.startsWith(`/${link.id}`);
            return (
              <motion.div key={link.id} whileHover={{ y: -1 }} whileTap={{ y: 0 }}>
                <Link
                  to={`/${link.id}`}
                  className={`relative px-3 py-1.5 text-sm font-medium transition-colors ${
                    isActive
                      ? darkMode
                        ? 'text-blue-400'
                        : 'text-blue-600'
                      : darkMode
                      ? 'text-gray-400 hover:text-gray-200'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  {link.name}

                  {/* Active underline */}
                  {isActive && (
                    <motion.div
                      layoutId="activeTab"
                      className={`absolute bottom-0 left-0 right-0 h-0.5 ${
                        darkMode ? 'bg-blue-400' : 'bg-blue-600'
                      }`}
                      initial={false}
                      transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                    />
                  )}
                </Link>
              </motion.div>
            );
          })}

          {/* Engineering Dropdown */}
          <DropdownMenu dropdownKey="engineering" dropdown={engineering} />

          {/* Resources Link */}
          <motion.div whileHover={{ y: -1 }} whileTap={{ y: 0 }}>
            <Link
              to="/resources"
              className={`relative px-3 py-1.5 text-sm font-medium transition-colors ${
                currentPath.startsWith('/resources')
                  ? darkMode
                    ? 'text-blue-400'
                    : 'text-blue-600'
                  : darkMode
                  ? 'text-gray-400 hover:text-gray-200'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Resources

              {/* Active underline */}
              {currentPath.startsWith('/resources') && (
                <motion.div
                  layoutId="activeTab"
                  className={`absolute bottom-0 left-0 right-0 h-0.5 ${
                    darkMode ? 'bg-blue-400' : 'bg-blue-600'
                  }`}
                  initial={false}
                  transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                />
              )}
            </Link>
          </motion.div>
        </div>

        {/* Mobile Navigation */}
        <AnimatePresence>
          {mobileMenuOpen && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="md:hidden overflow-hidden"
            >
              <div className="py-3 space-y-1" role="menu">
                {/* Top Level Links */}
                {topLevelLinks.map((link) => {
                  const isActive = currentPath.startsWith(`/${link.id}`);
                  return (
                    <Link
                      key={link.id}
                      to={`/${link.id}`}
                      className={`block px-4 py-2.5 font-medium transition-colors ${
                        isActive
                          ? darkMode
                            ? 'text-blue-400 bg-gray-800'
                            : 'text-blue-600 bg-gray-50'
                          : darkMode
                          ? 'text-gray-300 hover:bg-gray-800'
                          : 'text-gray-700 hover:bg-gray-50'
                      }`}
                    >
                      {link.name}
                    </Link>
                  );
                })}

                {/* Engineering Section */}
                <div className="pt-3">
                  <div className={`px-4 py-2 text-xs font-bold uppercase tracking-wider ${
                    darkMode ? 'text-gray-500' : 'text-gray-400'
                  }`}>
                    {engineering.name}
                  </div>
                  <div className="space-y-0.5">
                    {engineering.items.map((item) => {
                      const isActive = currentPath.startsWith(`/${item.id}`);
                      return (
                        <Link
                          key={item.id}
                          to={`/${item.id}`}
                          className={`block px-6 py-2.5 font-medium transition-colors ${
                            isActive
                              ? darkMode
                                ? 'text-blue-400 bg-gray-800'
                                : 'text-blue-600 bg-gray-50'
                              : darkMode
                              ? 'text-gray-300 hover:bg-gray-800'
                              : 'text-gray-700 hover:bg-gray-50'
                          }`}
                        >
                          {item.name}
                        </Link>
                      );
                    })}
                  </div>
                </div>

                {/* Resources Link */}
                <Link
                  to="/resources"
                  className={`block px-4 py-2.5 font-medium transition-colors ${
                    currentPath.startsWith('/resources')
                      ? darkMode
                        ? 'text-blue-400 bg-gray-800'
                        : 'text-blue-600 bg-gray-50'
                      : darkMode
                      ? 'text-gray-300 hover:bg-gray-800'
                      : 'text-gray-700 hover:bg-gray-50'
                  }`}
                >
                  Resources
                </Link>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </nav>
  );
}
