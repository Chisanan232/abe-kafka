import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import remarkGfm from 'remark-gfm';
import remarkMdxCodeMeta from 'remark-mdx-code-meta';

const config: Config = {
  title: 'Abstract Backend Implementation Template',
  tagline: '🐍 A reference implementation template for abstract-backend queue services.',
  favicon: 'img/python_logo_icon.png',

  // Set the production url of your site here
  url: 'https://chisanan232.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  baseUrl: '/abstract-backend-implementation-template/',
  projectName: 'abstract-backend-implementation-template',
  organizationName: 'Chisanan232',
  trailingSlash: false,

  onBrokenLinks: 'warn',
  onDuplicateRoutes: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    format: 'detect',
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
    mdx1Compat: {
      comments: true,
      admonitions: true,
      headingIds: true,
    },
  },

  presets: [
    [
      'classic',
      {
        docs: false,
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'docs',
        path: 'contents/document',
        routeBasePath: 'docs',
        sidebarPath: './contents/document/sidebars.ts',
        sidebarCollapsible: true,
        showLastUpdateTime: true,
        showLastUpdateAuthor: true,
        editUrl:
          'https://github.com/Chisanan232/abstract-backend-implementation-template/tree/master/docs/',
        versions: {
          current: {
            label: 'Next',
            path: 'next',
            banner: 'unreleased',
            badge: true,
          },
        },
        includeCurrentVersion: true,
        lastVersion: 'current',
      },
    ],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'dev',
        path: 'contents/development',
        routeBasePath: 'dev',
        sidebarPath: './contents/development/sidebars.ts',
        sidebarCollapsible: true,
        showLastUpdateTime: true,
        showLastUpdateAuthor: true,
        editUrl:
          'https://github.com/Chisanan232/abstract-backend-implementation-template/tree/master/docs/',
        versions: {
          current: {
            label: 'Next',
            path: 'next',
            banner: 'unreleased',
            badge: true,
          },
        },
        includeCurrentVersion: true,
        lastVersion: 'current',
      },
    ],
    [
      '@docusaurus/plugin-content-blog',
      {
        id: 'blog',
        path: 'contents/blog',
        routeBasePath: 'blog',
        showReadingTime: true,
        editUrl:
          'https://github.com/Chisanan232/abstract-backend-implementation-template/tree/master/docs/',
      },
    ],
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        // Options for docusaurus-search-local
        hashed: true,
        language: ['en'],
        docsRouteBasePath: ['/abstract-backend-implementation-template'],
        docsDir: ['./contents/document', './contents/development'],
        blogDir: ['./contents/blog'],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
        docsPluginIdForPreferredVersion: 'docs',
        indexDocs: true,
        indexBlog: true,
        indexPages: true,
      },
    ],
  ],

  themes: [
    '@docusaurus/theme-mermaid',
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/python_logo_icon.png',
    navbar: {
      title: 'Abstract Backend Template',
      logo: {
        alt: 'My Site Logo',
        src: 'img/python_logo_icon.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Docs',
          docsPluginId: 'docs',
        },
        {
          type: 'docSidebar',
          sidebarId: 'dev',
          position: 'left',
          label: 'Dev',
          docsPluginId: 'dev',
        },
        {
          to: '/blog',
          label: 'Blog',
          position: 'left',
        },
        {
          type: 'custom-unifiedVersions',
          position: 'right',
          pluginIds: ['docs', 'dev'],
          pluginTitles: {
            docs: 'User Guide',
            dev: 'Developer Guide',
          },
          showBadges: true,
          showDividers: true,
          showNextLabel: true,
          showUnmaintainedLabel: true,
          dropdownItemsBefore: [],
          dropdownItemsAfter: [],
        },
        {
          href: 'https://github.com/Chisanan232/abstract-backend-implementation-template',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Docs',
              to: '/docs/next/introduction',
            },
            {
              label: 'Dev',
              to: '/dev/next',
            },
            {
              label: 'Blog',
              to: '/blog',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub Issues',
              href: 'https://github.com/Chisanan232/abstract-backend-implementation-template/issues',
            },
            {
              label: 'GitHub Discussions',
              href: 'https://github.com/Chisanan232/abstract-backend-implementation-template/discussions',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub Repository',
              href: 'https://github.com/Chisanan232/abstract-backend-implementation-template',
            },
          ],
        },
      ],
      copyright: `Copyright ${new Date().getFullYear()} - PRESENT, Python-uv-project-template is owned by <a href="https://github.com/Chisanan232">@Chisanan232</a>.<br />Built with <a href="https://docusaurus.io/">Docusaurus</a>.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  },

  // Add redirect for homepage
  staticDirectories: ['static'],
  // Homepage redirects to documentation
  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'canonical',
        href: 'https://chisanan232.github.io/abstract-backend-implementation-template/docs/introduction',
      },
    },
  ],
};

export default config;
