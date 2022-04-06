export default {
  roots: ["<rootDir>/src"],
  testMatch: [
    "**/__tests__/**/*.+(ts|tsx|js)",
    "**/?(*.)+(spec|test).+(ts|tsx|js)",
  ],
  transform: {
    "^.+\\.(ts|tsx)$": "ts-jest",
  },
  transformIgnorePatterns: ["<rootDir>/node_modules/"],
  globalSetup: "<rootDir>/jest_setup.js",
  testEnvironment: "node",
  collectCoverage: true,
  collectCoverageFrom: ["**/*.ts", "!src/test-utils.ts"],
};
